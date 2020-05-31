package io.atomix.raft.snapshot.impl;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

import io.atomix.raft.snapshot.PersistedSnapshot;
import io.atomix.raft.snapshot.ReceivedSnapshot;
import io.atomix.raft.snapshot.SnapshotChunk;
import io.zeebe.util.FileUtil;
import io.zeebe.util.ZbLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

public class FileBasedReceivedSnapshot implements ReceivedSnapshot {

  private static final Logger LOGGER = new ZbLogger(FileBasedReceivedSnapshot.class);

  private final Path directory;
  private final FileBasedSnapshotStore snapshotStore;

  private ByteBuffer expectedId;
  private final FileBasedSnapshotMetadata metadata;

  FileBasedReceivedSnapshot(
      final FileBasedSnapshotMetadata metadata,
      final Path directory,
      final FileBasedSnapshotStore snapshotStore) {
    this.metadata = metadata;
    this.snapshotStore = snapshotStore;
    this.directory = directory;
  }

  @Override
  public long index() {
    return metadata.getIndex();
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

  @Override
  public boolean apply(final SnapshotChunk snapshotChunk) throws IOException {
    final String snapshotId = snapshotChunk.getSnapshotId();
    final String chunkName = snapshotChunk.getChunkName();

    if (snapshotStore.exists(snapshotId)) {
      LOGGER.debug(
          "Ignore snapshot snapshotChunk {}, because snapshot {} already exists.",
          chunkName,
          snapshotId);
      return true;
    }

    final long expectedChecksum = snapshotChunk.getChecksum();
    final long actualChecksum = SnapshotChunkUtil.createChecksum(snapshotChunk.getContent());

    if (expectedChecksum != actualChecksum) {
      LOGGER.warn(
          "Expected to have checksum {} for snapshot snapshotChunk file {} ({}), but calculated {}",
          expectedChecksum,
          chunkName,
          snapshotId,
          actualChecksum);
      return false;
    }

    final var tmpSnapshotDirectory = directory;
    FileUtil.ensureDirectoryExists(tmpSnapshotDirectory);

    final var snapshotFile = tmpSnapshotDirectory.resolve(chunkName);
    if (Files.exists(snapshotFile)) {
      LOGGER.debug("Received a snapshot snapshotChunk which already exist '{}'.", snapshotFile);
      return false;
    }

    LOGGER.debug("Consume snapshot snapshotChunk {} of snapshot {}", chunkName, snapshotId);
    return writeReceivedSnapshotChunk(snapshotChunk, snapshotFile);
  }

  private boolean writeReceivedSnapshotChunk(
      final SnapshotChunk snapshotChunk, final Path snapshotFile) throws IOException {
    Files.write(snapshotFile, snapshotChunk.getContent(), CREATE_NEW, StandardOpenOption.WRITE);
    LOGGER.trace("Wrote replicated snapshot chunk to file {}", snapshotFile);
    return true;
  }

  @Override
  public void setNextExpected(final ByteBuffer nextChunkId) {
    expectedId = nextChunkId;
  }

  @Override
  public PersistedSnapshot persist() {
    return snapshotStore.newSnapshot(metadata, directory);
  }

  @Override
  public void abort() {
    try {
      LOGGER.error("DELETE dir {}", directory);
      FileUtil.deleteFolder(directory);
    } catch (final IOException e) {
      LOGGER.warn("Failed to delete pending snapshot {}", this, e);
    }
  }

  public Path getPath() {
    return directory;
  }

  private String getFile(final ByteBuffer chunkId) {
    final var view = new UnsafeBuffer(chunkId);
    return view.getStringWithoutLengthAscii(0, chunkId.remaining());
  }

  @Override
  public String toString() {
    return "FileBasedReceivedSnapshot{" +
        "directory=" + directory +
        ", snapshotStore=" + snapshotStore +
        ", expectedId=" + expectedId +
        ", metadata=" + metadata +
        '}';
  }
}
