/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.raft.storage.snapshot.Snapshot;
import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.broker.snapshot.impl.DirBasedSnapshotStore;
import io.zeebe.broker.snapshot.impl.DirBasedSnapshotStoreFactory;
import java.util.ArrayList;
import org.agrona.IoUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class DirBasedSnapshotStoreFactoryTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void shouldCreateDirectoriesIfNotExist() {
    // given
    final var root = temporaryFolder.getRoot().toPath();
    final var factory = new DirBasedSnapshotStoreFactory();

    // when
    final var store = factory.createSnapshotStore(root, "ignored");

    // then
    assertThat(root.resolve(DirBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY))
        .exists()
        .isDirectory();
    assertThat(root.resolve(DirBasedSnapshotStoreFactory.PENDING_DIRECTORY)).exists().isDirectory();
    assertThat(store.getSnapshots()).isEmpty();
  }

  @Test
  public void shouldLoadExistingSnapshots() {
    // given
    final var root = temporaryFolder.getRoot().toPath();
    final var factory = new DirBasedSnapshotStoreFactory();
    final var originalStore = (DirBasedSnapshotStore) factory.createSnapshotStore(root, "ignored");
    final var firstSnapshot = newCommittedSnapshot(originalStore, 1);
    final var secondSnapshot = newCommittedSnapshot(originalStore, 2);

    // when
    final var store = factory.createSnapshotStore(root, "ignored");

    // then
    final var snapshots = new ArrayList<Snapshot>(store.getSnapshots());
    assertThat(snapshots).hasSize(2).containsExactly(firstSnapshot, secondSnapshot);
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
}
