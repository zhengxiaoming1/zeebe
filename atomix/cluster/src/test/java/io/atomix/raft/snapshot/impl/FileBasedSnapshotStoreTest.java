/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.snapshot.impl;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.atomix.raft.snapshot.PersistedSnapshotListener;
import io.atomix.raft.snapshot.PersistedSnapshotStore;
import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileBasedSnapshotStoreTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private PersistedSnapshotStore persistedSnapshotStore;
  private Path snapshotsDir;
  private Path pendingSnapshotsDir;

  @Before
  public void before() {
    final FileBasedSnapshotStoreFactory factory = new FileBasedSnapshotStoreFactory();

    final var partitionName = "1";
    final var root = temporaryFolder.getRoot();

    persistedSnapshotStore = factory.createSnapshotStore(root.toPath(), partitionName);

    snapshotsDir =
        temporaryFolder
            .getRoot()
            .toPath()
            .resolve(FileBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY);
    pendingSnapshotsDir =
        temporaryFolder.getRoot().toPath().resolve(FileBasedSnapshotStoreFactory.PENDING_DIRECTORY);
  }

  @Test
  public void shouldCreateSubFoldersOnCreatingDirBasedStore() {
    // given

    // when + then
    assertThat(snapshotsDir).exists();
    Assertions.assertThat(pendingSnapshotsDir).exists();
  }

  @Test
  public void shouldDeleteStore() {
    // given

    // when
    persistedSnapshotStore.delete();

    // then
    assertThat(pendingSnapshotsDir).doesNotExist();
    assertThat(snapshotsDir).doesNotExist();
  }

  @Test
  public void shouldNotCreateDirForTakeTransientSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);

    // when
    persistedSnapshotStore.newTransientSnapshot(index, term, time);

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldBeAbleToAbortNotStartedSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, time);

    // when
    transientSnapshot.abort();

    // then
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldTakeTransientSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, time);

    // when
    transientSnapshot.take(this::createSnapshotDir);

    // then
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
    final var snapshotDirs = pendingSnapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var pendingSnapshotDir = snapshotDirs[0];
    assertThat(pendingSnapshotDir.getName()).isEqualTo("1-0-123");
    assertThat(pendingSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldAbortAndDeleteTransientSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, time);
    transientSnapshot.take(this::createSnapshotDir);

    // when
    transientSnapshot.abort();

    // then
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldCommitTakenSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, time);
    transientSnapshot.take(this::createSnapshotDir);

    // when
    final var persistedSnapshot = transientSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo("1-0-123");
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldReplaceSnapshotOnNextSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, time);
    oldTransientSnapshot.take(this::createSnapshotDir);
    oldTransientSnapshot.persist();

    // when
    final var newSnapshot = persistedSnapshotStore.newTransientSnapshot(index + 1, term, time);
    newSnapshot.take(this::createSnapshotDir);
    newSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo("2-0-123");
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldRemovePendingSnapshotOnCommittingSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, time);
    oldTransientSnapshot.take(this::createSnapshotDir);

    // when
    final var newSnapshot = persistedSnapshotStore.newTransientSnapshot(index + 1, term, time);
    newSnapshot.take(this::createSnapshotDir);
    newSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo("2-0-123");
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldCleanUpPendingDirOnFailingTakeSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, time);

    // when
    oldTransientSnapshot.take(
        path -> {
          try {
            FileUtil.ensureDirectoryExists(path);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
          return false;
        });

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldCleanUpPendingDirOnException() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, time);

    // when
    oldTransientSnapshot.take(
        path -> {
          try {
            FileUtil.ensureDirectoryExists(path);
            throw new RuntimeException("EXPECTED");
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldNotifyListenersOnNewSnapshot() {
    // given
    final var listener = mock(PersistedSnapshotListener.class);
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, time);
    persistedSnapshotStore.addSnapshotListener(listener);
    transientSnapshot.take(this::createSnapshotDir);

    // when
    final var persistedSnapshot = transientSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    verify(listener, times(1)).onNewSnapshot(eq(persistedSnapshot));
  }

  @Test
  public void shouldNotNotifyListenersOnNewSnapshotWhenDeregistered() {
    // given
    final var listener = mock(PersistedSnapshotListener.class);
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, time);
    persistedSnapshotStore.addSnapshotListener(listener);
    persistedSnapshotStore.removeSnapshotListener(listener);
    transientSnapshot.take(this::createSnapshotDir);

    // when
    final var persistedSnapshot = transientSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    verify(listener, times(0)).onNewSnapshot(eq(persistedSnapshot));
  }


//
//  @Test
//  public void shouldReturnExistingIfSnapshotAlreadyExists() {
//    // given
//    final var directory = pendingDirectory.resolve("1-1-1-1");
//    final var store = newStore();
//    IoUtil.ensureDirectoryExists(directory.toFile(), "snapshot directory");
//
//    // when
//    final var snapshot = store.newSnapshot(1, 1, WallClockTimestamp.from(1), directory);
//    final var existing =
//        store.newSnapshot(
//            snapshot.index(),
//            snapshot.term(),
//            snapshot.timestamp(),
//            pendingDirectory.resolve("1-1-1-1"));
//
//    // then
//    assertThat(existing).isEqualTo(snapshot);
//  }
//
//  @Test
//  public void shouldDeleteOrphanedPendingSnapshots() throws IOException {
//    // given
//    final var pendingSnapshots =
//        List.of(pendingDirectory.resolve("1-1-1"), pendingDirectory.resolve("2-2-2"));
//    final var store = newStore();
//    pendingSnapshots.forEach(p -> IoUtil.ensureDirectoryExists(p.toFile(), ""));
//
//    // when
//    store.purgePendingSnapshots();
//
//    // then
//    pendingSnapshots.forEach(p -> assertThat(p).doesNotExist());
//  }
//
//  @Test
//  public void shouldDeleteOrphanedPendingSnapshotsOnNewSnapshot() throws IOException {
//    // given
//    final var pendingSnapshots =
//        List.of(pendingDirectory.resolve("1-1-1"), pendingDirectory.resolve("2-2-2"));
//    final var store = newStore();
//    pendingSnapshots.forEach(p -> IoUtil.ensureDirectoryExists(p.toFile(), ""));
//
//    // when
//    final var snapshot =
//        store.newSnapshot(2, 2, WallClockTimestamp.from(2), pendingDirectory.resolve("2-2-2"));
//
//    // then
//    pendingSnapshots.forEach(p -> assertThat(p).doesNotExist());
//    assertThat(snapshot.getPath()).exists();
//  }
//
//  @Test
//  public void shouldNotDeleteHigherPendingSnapshotsOnNewSnapshot() {
//    // given
//    final var pendingSnapshots =
//        List.of(pendingDirectory.resolve("1-1-1"), pendingDirectory.resolve("2-2-2"));
//    final var store = newStore();
//    pendingSnapshots.forEach(p -> IoUtil.ensureDirectoryExists(p.toFile(), ""));
//
//    // when
//    final var snapshot =
//        store.newSnapshot(1, 1, WallClockTimestamp.from(1), pendingDirectory.resolve("1-1-1"));
//
//    // then
//    assertThat(pendingDirectory.resolve("2-2-2")).exists();
//    assertThat(pendingDirectory.resolve("1-1-1")).doesNotExist();
//    assertThat(snapshot.getPath()).exists();
//  }
//
//  @Test
//  public void shouldMoveSnapshot() {
//    // given
//    final var toDeleteDirectory = pendingDirectory.resolve("1-1-1-1");
//    final var store = newStore();
//    IoUtil.ensureDirectoryExists(toDeleteDirectory.toFile(), "snapshot directory");
//
//    // when
//    final var snapshot = store.newSnapshot(1, 1, WallClockTimestamp.from(1), toDeleteDirectory);
//
//    // then
//    assertThat(toDeleteDirectory).doesNotExist();
//    assertThat(snapshot.getPath()).exists();
//  }
//
//  @Test
//  public void shouldDeleteOlderSnapshotsOnNextSnapshot() {
//    // given
//    final var originalStore = newStore();
//    final var firstSnapshot = newCommittedSnapshot(originalStore, 1);
//    assertThat(firstSnapshot.getPath()).exists();
//
//    // when
//    final var secondSnapshot = newCommittedSnapshot(originalStore, 2);
//
//    // then
//    assertThat(store.getLatestSnapshot()).isPresent();
//    assertThat(store.getLatestSnapshot()).get().isEqualTo(secondSnapshot);
//    assertThat(firstSnapshot.getPath()).doesNotExist();
//  }
//
//  @Test
//  public void shouldLoadExistingSnapshots() {
//    // given
//    final var originalStore = newStore();
//    newCommittedSnapshot(originalStore, 1); // firstSnapshot
//    final var secondSnapshot = newCommittedSnapshot(originalStore, 2);
//
//    // when
//    final var store = newStore();
//
//    // then
//    assertThat(store.getLatestSnapshot()).isPresent();
//    assertThat(store.getLatestSnapshot()).get().isEqualTo(secondSnapshot);
//  }

  private boolean createSnapshotDir(final Path path) {
    try {
      FileUtil.ensureDirectoryExists(path);
      Files.write(
          path.resolve("file1.txt"),
          "This is the content".getBytes(),
          CREATE_NEW,
          StandardOpenOption.WRITE);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return true;
  }
}
