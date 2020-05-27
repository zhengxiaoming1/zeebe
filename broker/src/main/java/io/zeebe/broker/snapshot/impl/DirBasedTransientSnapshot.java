/*
 * Copyright © 2020  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package io.zeebe.broker.snapshot.impl;

import io.atomix.raft.snapshot.Snapshot;
import io.atomix.raft.snapshot.SnapshotChunk;
import io.atomix.raft.snapshot.TransientSnapshot;
import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.util.FileUtil;
import io.zeebe.util.ZbLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

/**
 * Represents a pending snapshot, that is a snapshot in the process of being written and has not yet
 * been committed to the store.
 */
public final class DirBasedTransientSnapshot implements TransientSnapshot {
  private static final Logger LOGGER = new ZbLogger(DirBasedTransientSnapshot.class);

  private final long index;
  private final long term;
  private final WallClockTimestamp timestamp;

  private final Path directory;
  private final DirBasedSnapshotStore snapshotStore;

  private ByteBuffer expectedId;

  /**
   * @param index the snapshot's index
   * @param term the snapshot's term
   * @param timestamp the snapshot's creation timestamp
   * @param directory the snapshot's working directory (i.e. where we should write chunks)
   * @param snapshotStore the store which will be called when the snapshot is to be committed
   */
  DirBasedTransientSnapshot(
      final long index,
      final long term,
      final WallClockTimestamp timestamp,
      final Path directory,
      final DirBasedSnapshotStore snapshotStore) {
    this.index = index;
    this.term = term;
    this.timestamp = timestamp;
    this.directory = directory;
    this.snapshotStore = snapshotStore;
  }

  @Override
  public void take(final Predicate<Path> takeSnapshot) {

    // todo metrics
    if (!takeSnapshot.test(getPath()))
    {
      // todo(zell): before we did nothing?!
      abort();
    }
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public WallClockTimestamp timestamp() {
    return timestamp;
  }

  @Override
  public boolean containsChunk(final ByteBuffer chunkId) {
    return Files.exists(directory.resolve(getFile(chunkId)));
  }

  @Override
  public boolean isExpectedChunk(final ByteBuffer chunkId) {
    if (expectedId == null) {
      return chunkId == null;
    }

    return expectedId.equals(chunkId);
  }

  //  @Override
  //  public void write(final ByteBuffer chunkId, final ByteBuffer chunkData) {
  //    final var filename = getFile(chunkId);
  //    final var path = directory.resolve(filename);
  //
  //    try {
  //      FileUtil.ensureDirectoryExists(directory);
  //    } catch (final IOException e) {
  //      LOGGER.error("Failed to ensure pending snapshot directory {} exists", directory, e);
  //      throw new UncheckedIOException(e);
  //    }
  //
  //    try (final var channel =
  //        Files.newByteChannel(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
  //      final var expectedToWrite = chunkData.remaining();
  //      long actualWrittenBytes = 0L;
  //      while (chunkData.hasRemaining()) {
  //        actualWrittenBytes += channel.write(chunkData);
  //      }
  //
  //      if (actualWrittenBytes != expectedToWrite) {
  //        throw new IllegalStateException(
  //            "Expected to write "
  //                + expectedToWrite
  //                + " bytes of the given snapshot chunk with id "
  //                + chunkId
  //                + ", but only "
  //                + actualWrittenBytes
  //                + " bytes were written");
  //      }
  //    } catch (final FileAlreadyExistsException e) {
  //      LOGGER.debug("Chunk {} of pending snapshot {} already exists at {}", filename, this, path,
  // e);
  //    } catch (final IOException e) {
  //      throw new UncheckedIOException(e);
  //    }
  //  }

  @Override
  public void write(final SnapshotChunk chunk) {}

  @Override
  public void setNextExpected(final ByteBuffer nextChunkId) {
    expectedId = nextChunkId;
  }

  @Override
  public Snapshot commit() {
    return snapshotStore.newSnapshot(index, term, timestamp, directory);
  }

  @Override
  public void abort() {
    try {
      FileUtil.deleteFolder(directory);
    } catch (final IOException e) {
      LOGGER.warn("Failed to delete pending snapshot {}", this, e);
    }
  }

  @Override
  public Path getPath() {
    return directory;
  }

  @Override
  public String toString() {
    return "DbPendingSnapshot{"
        + "index="
        + index
        + ", term="
        + term
        + ", timestamp="
        + timestamp
        + ", directory="
        + directory
        + '}';
  }

  private String getFile(final ByteBuffer chunkId) {
    final var view = new UnsafeBuffer(chunkId);
    return view.getStringWithoutLengthAscii(0, chunkId.remaining());
  }
}
