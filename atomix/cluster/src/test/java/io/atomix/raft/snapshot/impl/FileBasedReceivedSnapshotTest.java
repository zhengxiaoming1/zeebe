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

import io.atomix.raft.snapshot.PersistedSnapshot;
import io.atomix.raft.snapshot.PersistedSnapshotListener;
import io.atomix.raft.snapshot.PersistedSnapshotStore;
import io.atomix.raft.snapshot.ReceivedSnapshot;
import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileBasedReceivedSnapshotTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private PersistedSnapshotStore senderSnapshotStore;
  private PersistedSnapshotStore receiverSnapshotStore;
  private Path receiverSnapshotsDir;
  private Path receiverPendingSnapshotsDir;
  private FileBasedSnapshotStoreFactory factory;

  @Before
  public void before() throws Exception {
    factory = new FileBasedSnapshotStoreFactory();
    final String partitionName = "1";
    final File senderRoot = temporaryFolder.newFolder("sender");

    senderSnapshotStore = factory.createSnapshotStore(senderRoot.toPath(), partitionName);

    final var receiverRoot = temporaryFolder.newFolder("received");
    receiverSnapshotStore = factory.createSnapshotStore(receiverRoot.toPath(), partitionName);

    receiverSnapshotsDir =
        receiverRoot.toPath().resolve(FileBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY);
    receiverPendingSnapshotsDir =
        receiverRoot.toPath().resolve(FileBasedSnapshotStoreFactory.PENDING_DIRECTORY);
  }

  @Test
  public void shouldNotCreateDirDirectlyOnNewReceivedSnapshot() {
    // given

    // when
    senderSnapshotStore.newReceivedSnapshot("1-0-123");

    // then
    assertThat(receiverPendingSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(receiverSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldWriteChunkInPendingDirOnApplyChunk() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    takeAndReceiveSnapshot(index, term, time);

    // then
    assertThat(receiverSnapshotsDir.toFile().listFiles()).isEmpty();

    assertThat(receiverPendingSnapshotsDir).exists();
    final var files = receiverPendingSnapshotsDir.toFile().listFiles();
    assertThat(files).isNotNull().hasSize(1);

    final var dir = files[0];
    assertThat(dir).hasName("1-0-123");

    final var snapshotFileList = dir.listFiles();
    assertThat(snapshotFileList).isNotNull().extracting(File::getName).containsExactly("file1.txt");
  }

  @Test
  public void shouldDeletePendingSnapshotDirOnAbort() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var transientSnapshot = senderSnapshotStore.newTransientSnapshot(index, term, time);
    transientSnapshot.take(this::takeSnapshot);
    final var persistedSnapshot = transientSnapshot.persist();

    final var receivedSnapshot =
        receiverSnapshotStore.newReceivedSnapshot(
            persistedSnapshot.getId().getSnapshotIdAsString());
    try (final var snapshotChunkReader = persistedSnapshot.newChunkReader()) {
      while (snapshotChunkReader.hasNext()) {
        receivedSnapshot.apply(snapshotChunkReader.next());
      }
    }

    // when
    receivedSnapshot.abort();

    // then
    assertThat(receiverSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(receiverPendingSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldPurgePendingOnStore() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    takeAndReceiveSnapshot(index, term, time);

    // when
    receiverSnapshotStore.purgePendingSnapshots();

    // then
    assertThat(receiverSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(receiverPendingSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldPersistSnapshot() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    final var receivedSnapshot = takeAndReceiveSnapshot(index, term, time);

    // when
    final var snapshot = receivedSnapshot.persist();

    // then
    assertThat(snapshot).isNotNull();
    assertThat(receiverPendingSnapshotsDir.toFile().listFiles()).isEmpty();

    assertThat(receiverSnapshotsDir).exists();
    final var files = receiverSnapshotsDir.toFile().listFiles();
    assertThat(files).isNotNull().hasSize(1);

    final var dir = files[0];
    assertThat(dir).hasName("1-0-123");

    final var snapshotFileList = dir.listFiles();
    assertThat(snapshotFileList).isNotNull().extracting(File::getName).containsExactly("file1.txt");
  }

  @Test
  public void shouldNotDeletePersistedSnapshotOnPurgePendingOnStore() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    takeAndReceiveSnapshot(index, term, time).persist();

    // when
    receiverSnapshotStore.purgePendingSnapshots();

    // then
    assertThat(receiverPendingSnapshotsDir.toFile().listFiles()).isEmpty();

    assertThat(receiverSnapshotsDir).exists();
    final var files = receiverSnapshotsDir.toFile().listFiles();
    assertThat(files).isNotNull().hasSize(1);

    final var dir = files[0];
    assertThat(dir).hasName("1-0-123");

    final var snapshotFileList = dir.listFiles();
    assertThat(snapshotFileList).isNotNull().extracting(File::getName).containsExactly("file1.txt");
  }

  @Test
  public void shouldReplaceSnapshotOnNextSnapshot() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    takeAndReceiveSnapshot(index, term, time).persist();

    // when
    takeAndReceiveSnapshot(index + 1, term, time).persist();

    // then
    assertThat(receiverPendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = receiverSnapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo("2-0-123");
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldRemovePendingSnapshotOnCommittingSnapshot() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    takeAndReceiveSnapshot(index, term, time);

    // when
    takeAndReceiveSnapshot(index + 1, term, time).persist();

    // then
    assertThat(receiverPendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = receiverSnapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo("2-0-123");
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldNotRemovePendingSnapshotOnCommittingSnapshotWhenHigher() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);

    final var otherStore =
        factory.createSnapshotStore(temporaryFolder.newFolder("other").toPath(), "1");
    final var olderTransient = otherStore.newTransientSnapshot(index, term, time);
    olderTransient.take(this::takeSnapshot);
    final var olderPersistedSnapshot = olderTransient.persist();

    final var newTransient = senderSnapshotStore.newTransientSnapshot(index + 1, term, time);
    newTransient.take(this::takeSnapshot);
    final var newPersistedSnapshot = newTransient.persist();

    receiveSnapshot(newPersistedSnapshot);

    // when
    receiveSnapshot(olderPersistedSnapshot).persist();

    // then
    final var pendingSnapshotDirs = receiverPendingSnapshotsDir.toFile().listFiles();
    assertThat(pendingSnapshotDirs).isNotNull().hasSize(1);

    final var pendingSnapshotDir = pendingSnapshotDirs[0];
    assertThat(pendingSnapshotDir.getName()).isEqualTo("2-0-123");
    assertThat(pendingSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");

    final var snapshotDirs = receiverSnapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo("1-0-123");
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldNotifyListenersOnNewSnapshot() throws Exception {
    // given
    final var listener = mock(PersistedSnapshotListener.class);
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    receiverSnapshotStore.addSnapshotListener(listener);

    // when
    final var persistedSnapshot = takeAndReceiveSnapshot(index, term, time).persist();

    // then
    verify(listener, times(1)).onNewSnapshot(eq(persistedSnapshot));
  }

  @Test
  public void shouldNotNotifyListenersOnNewSnapshotWhenDeregistered() throws Exception {
    // given
    final var listener = mock(PersistedSnapshotListener.class);
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);
    senderSnapshotStore.addSnapshotListener(listener);
    senderSnapshotStore.removeSnapshotListener(listener);

    // when
    final var persistedSnapshot = takeAndReceiveSnapshot(index, term, time).persist();

    // then
    verify(listener, times(0)).onNewSnapshot(eq(persistedSnapshot));
  }

  private ReceivedSnapshot takeAndReceiveSnapshot(
      final long index, final long term, final WallClockTimestamp time) throws IOException {
    final var transientSnapshot = senderSnapshotStore.newTransientSnapshot(index, term, time);
    transientSnapshot.take(this::takeSnapshot);
    final var persistedSnapshot = transientSnapshot.persist();

    return receiveSnapshot(persistedSnapshot);
  }

  private ReceivedSnapshot receiveSnapshot(final PersistedSnapshot persistedSnapshot)
      throws IOException {
    final var receivedSnapshot =
        receiverSnapshotStore.newReceivedSnapshot(
            persistedSnapshot.getId().getSnapshotIdAsString());

    try (final var snapshotChunkReader = persistedSnapshot.newChunkReader()) {
      while (snapshotChunkReader.hasNext()) {
        receivedSnapshot.apply(snapshotChunkReader.next());
      }
    }

    return receivedSnapshot;
  }

  private boolean takeSnapshot(final Path path) {
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
