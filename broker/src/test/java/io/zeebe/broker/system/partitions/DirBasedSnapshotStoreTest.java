/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.atomix.raft.impl.zeebe.snapshot.SnapshotMetrics;
import io.atomix.raft.snapshot.Snapshot;
import io.atomix.raft.snapshot.SnapshotListener;
import io.atomix.raft.snapshot.SnapshotStore;
import io.atomix.raft.snapshot.impl.DirBasedSnapshotStore;
import io.atomix.raft.snapshot.impl.DirBasedSnapshotStoreFactory;
import io.atomix.utils.time.WallClockTimestamp;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class DirBasedSnapshotStoreTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path snapshotsDirectory;
  private Path pendingDirectory;
  private DirBasedSnapshotStore store;

  @Before
  public void setUp() throws Exception {
    snapshotsDirectory = temporaryFolder.newFolder("snapshots").toPath();
    pendingDirectory = temporaryFolder.newFolder("pending").toPath();
  }

  @After
  public void tearDown() {
    Optional.ofNullable(store).ifPresent(SnapshotStore::close);
  }

  @Test
  public void shouldNotifyListenersOnNewSnapshot() {
    // given
    final var directory = pendingDirectory.resolve("1-1-1-1");
    final var listener = mock(SnapshotListener.class);
    final var store = newStore();
    store.addSnapshotListener(listener);
    IoUtil.ensureDirectoryExists(directory.toFile(), "snapshot directory");

    // when
    final var snapshot = store.newSnapshot(1, 1, WallClockTimestamp.from(1), directory);

    // then
    verify(listener, times(1)).onNewSnapshot(eq(snapshot));
  }

  @Test
  public void shouldDeleteStore() {
    // given
    final var store = newStore();

    // when
    store.delete();

    // then
    assertThat(pendingDirectory).doesNotExist();
    assertThat(snapshotsDirectory).doesNotExist();
  }

  @Test
  public void shouldReturnExistingIfSnapshotAlreadyExists() {
    // given
    final var directory = pendingDirectory.resolve("1-1-1-1");
    final var store = newStore();
    IoUtil.ensureDirectoryExists(directory.toFile(), "snapshot directory");

    // when
    final var snapshot = store.newSnapshot(1, 1, WallClockTimestamp.from(1), directory);
    final var existing =
        store.newSnapshot(
            snapshot.index(),
            snapshot.term(),
            snapshot.timestamp(),
            pendingDirectory.resolve("1-1-1-1"));

    // then
    assertThat(existing).isEqualTo(snapshot);
  }

  @Test
  public void shouldDeleteOrphanedPendingSnapshots() throws IOException {
    // given
    final var pendingSnapshots =
        List.of(pendingDirectory.resolve("1-1-1"), pendingDirectory.resolve("2-2-2"));
    final var store = newStore();
    pendingSnapshots.forEach(p -> IoUtil.ensureDirectoryExists(p.toFile(), ""));

    // when
    store.purgePendingSnapshots();

    // then
    pendingSnapshots.forEach(p -> assertThat(p).doesNotExist());
  }

  @Test
  public void shouldDeleteOrphanedPendingSnapshotsOnNewSnapshot() throws IOException {
    // given
    final var pendingSnapshots =
        List.of(pendingDirectory.resolve("1-1-1"), pendingDirectory.resolve("2-2-2"));
    final var store = newStore();
    pendingSnapshots.forEach(p -> IoUtil.ensureDirectoryExists(p.toFile(), ""));

    // when
    final var snapshot =
        store.newSnapshot(2, 2, WallClockTimestamp.from(2), pendingDirectory.resolve("2-2-2"));

    // then
    pendingSnapshots.forEach(p -> assertThat(p).doesNotExist());
    assertThat(snapshot.getPath()).exists();
  }

  @Test
  public void shouldNotDeleteHigherPendingSnapshotsOnNewSnapshot() {
    // given
    final var pendingSnapshots =
        List.of(pendingDirectory.resolve("1-1-1"), pendingDirectory.resolve("2-2-2"));
    final var store = newStore();
    pendingSnapshots.forEach(p -> IoUtil.ensureDirectoryExists(p.toFile(), ""));

    // when
    final var snapshot =
        store.newSnapshot(1, 1, WallClockTimestamp.from(1), pendingDirectory.resolve("1-1-1"));

    // then
    assertThat(pendingDirectory.resolve("2-2-2")).exists();
    assertThat(pendingDirectory.resolve("1-1-1")).doesNotExist();
    assertThat(snapshot.getPath()).exists();
  }

  @Test
  public void shouldMoveSnapshot() {
    // given
    final var toDeleteDirectory = pendingDirectory.resolve("1-1-1-1");
    final var store = newStore();
    IoUtil.ensureDirectoryExists(toDeleteDirectory.toFile(), "snapshot directory");

    // when
    final var snapshot = store.newSnapshot(1, 1, WallClockTimestamp.from(1), toDeleteDirectory);

    // then
    assertThat(toDeleteDirectory).doesNotExist();
    assertThat(snapshot.getPath()).exists();
  }

  @Test
  public void shouldDeleteOlderSnapshotsOnNextSnapshot() {
    // given
    final var originalStore = newStore();
    final var firstSnapshot = newCommittedSnapshot(originalStore, 1);
    assertThat(firstSnapshot.getPath()).exists();

    // when
    final var secondSnapshot = newCommittedSnapshot(originalStore, 2);

    // then
    assertThat(store.getLatestSnapshot()).isPresent();
    assertThat(store.getLatestSnapshot()).get().isEqualTo(secondSnapshot);
    assertThat(firstSnapshot.getPath()).doesNotExist();
  }

  @Test
  public void shouldLoadExistingSnapshots() {
    // given
    final var originalStore = newStore();
    newCommittedSnapshot(originalStore, 1); // firstSnapshot
    final var secondSnapshot = newCommittedSnapshot(originalStore, 2);

    // when
    final var store = newStore();

    // then
    assertThat(store.getLatestSnapshot()).isPresent();
    assertThat(store.getLatestSnapshot()).get().isEqualTo(secondSnapshot);
  }

  private Snapshot newCommittedSnapshot(final DirBasedSnapshotStore store, final long index) {
    final var directory =
        store
            .getPath()
            .resolveSibling(DirBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY)
            .resolve(String.format("%d-1-1-1", index));
    IoUtil.ensureDirectoryExists(directory.toFile(), "snapshot directory " + index);
    return store.newSnapshot(index, 1, WallClockTimestamp.from(1), directory);
  }

  private DirBasedSnapshotStore newStore() {
    store =
        new DirBasedSnapshotStore(new SnapshotMetrics("1"), snapshotsDirectory, pendingDirectory);
    return store;
  }
}
