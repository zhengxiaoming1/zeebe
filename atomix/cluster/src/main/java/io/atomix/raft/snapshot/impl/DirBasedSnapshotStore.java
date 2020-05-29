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
package io.atomix.raft.snapshot.impl;

import io.atomix.raft.snapshot.Snapshot;
import io.atomix.raft.snapshot.SnapshotListener;
import io.atomix.raft.snapshot.SnapshotStore;
import io.atomix.raft.snapshot.TransientSnapshot;
import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.util.FileUtil;
import io.zeebe.util.ZbLogger;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.slf4j.Logger;

public final class DirBasedSnapshotStore implements SnapshotStore {

  private static final Logger LOGGER = new ZbLogger(DirBasedSnapshotStore.class);

  // the root snapshotsDirectory where all snapshots should be stored
  private final Path snapshotsDirectory;
  // the root snapshotsDirectory when pending snapshots should be stored
  private final Path pendingDirectory;
  // keeps track of all snapshot modification listeners
  private final Set<SnapshotListener> listeners;

  private final SnapshotMetrics snapshotMetrics;

  private Snapshot currentSnapshot;

  public DirBasedSnapshotStore(
      final SnapshotMetrics snapshotMetrics,
      final Path snapshotsDirectory,
      final Path pendingDirectory) {
    this.snapshotsDirectory = snapshotsDirectory;
    this.pendingDirectory = pendingDirectory;
    this.snapshotMetrics = snapshotMetrics;

    this.listeners = new CopyOnWriteArraySet<>();

    // load previous snapshots
    currentSnapshot = loadLatestSnapshot(snapshotsDirectory);
  }

  private Snapshot loadLatestSnapshot(final Path snapshotDirectory) {
    Snapshot latestSnapshot = null;
    try (final var stream = Files.newDirectoryStream(snapshotDirectory)) {
      for (final var path : stream) {
        latestSnapshot = collectSnapshot(path);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return latestSnapshot;
  }

  private Snapshot collectSnapshot(final Path path) {
    final var optionalMeta = DirBasedSnapshotMetadata.ofPath(path);
    if (optionalMeta.isPresent()) {
      final var metadata = optionalMeta.get();
      return new DirBasedSnapshot(path, metadata);
    } else {
      LOGGER.warn("Expected snapshot file format to be %d-%d-%d-%d, but was {}", path);
    }
    return null;
  }

  @Override
  public boolean exists(final String id) {
    final var optLatestSnapshot = getLatestSnapshot();

    if (optLatestSnapshot.isPresent()) {
      final var snapshot = optLatestSnapshot.get();
      return snapshot.getPath().getFileName().toString().equals(id);
    }
    return false;
  }

  @Override
  public TransientSnapshot takeTransientSnapshot(
      final long index, final long term, final WallClockTimestamp timestamp) {
    final var directory = buildPendingSnapshotDirectory(index, term, timestamp);
    return new DirBasedTransientSnapshot(index, term, timestamp, directory, this);
  }

  @Override
  public TransientSnapshot takeTransientSnapshot(final String snapshotId) {
    final var optMetadata = DirBasedSnapshotMetadata.ofFileName(snapshotId);
    final var metadata = optMetadata.orElseThrow();

    final var pendingSnapshotDir = pendingDirectory.resolve(metadata.getFileName());
    return new DirBasedTransientSnapshot(metadata, pendingSnapshotDir, this);
  }

  @Override
  public Optional<Snapshot> getLatestSnapshot() {
    return Optional.ofNullable(currentSnapshot);
  }

  @Override
  public void purgePendingSnapshots() throws IOException {
    try (final var files = Files.list(pendingDirectory)) {
      files.filter(Files::isDirectory).forEach(this::purgePendingSnapshot);
    }
  }

  @Override
  public void addSnapshotListener(final SnapshotListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeSnapshotListener(final SnapshotListener listener) {
    listeners.remove(listener);
  }

  @Override
  public long getCurrentSnapshotIndex() {
    return getLatestSnapshot().map(Snapshot::index).orElse(0L);
  }

  @Override
  public void delete() {
    // currently only called by Atomix when permanently leaving a cluster - it should be safe here
    // to not update the metrics, as they will simply disappear as time moves on. Once we have a
    // single store/replication mechanism, we can consider updating the metrics here
    currentSnapshot = null;
    //    snapshotMetrics.decrementSnapshotCount();

    try {
      LOGGER.error("DELETE FOLDER {}", snapshotsDirectory);
      FileUtil.deleteFolder(snapshotsDirectory);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    try {
      LOGGER.error("DELETE FOLDER {}", snapshotsDirectory);
      FileUtil.deleteFolder(pendingDirectory);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void observeSnapshotSize(final Snapshot snapshot) {
    try (final var contents = Files.newDirectoryStream(snapshot.getPath())) {
      var totalSize = 0L;

      for (final var path : contents) {
        if (Files.isRegularFile(path)) {
          final var size = Files.size(path);
          snapshotMetrics.observeSnapshotFileSize(size);
          totalSize += size;
        }
      }

      snapshotMetrics.observeSnapshotSize(totalSize);
    } catch (final IOException e) {
      LOGGER.warn("Failed to observe size for snapshot {}", snapshot, e);
    }
  }

  private void purgePendingSnapshots(final long cutoffIndex) {
    LOGGER.debug(
        "Search for orphaned snapshots below oldest valid snapshot with index {} in {}",
        cutoffIndex,
        pendingDirectory);

    try (final var pendingSnapshots = Files.newDirectoryStream(pendingDirectory)) {
      for (final var pendingSnapshot : pendingSnapshots) {
        purgePendingSnapshot(cutoffIndex, pendingSnapshot);
      }
    } catch (final IOException e) {
      LOGGER.warn(
          "Failed to delete orphaned snapshots, could not list pending directory {}",
          pendingDirectory);
    }
  }

  private void purgePendingSnapshot(final long cutoffIndex, final Path pendingSnapshot) {
    final var optionalMetadata = DirBasedSnapshotMetadata.ofPath(pendingSnapshot);
    if (optionalMetadata.isPresent() && optionalMetadata.get().getIndex() < cutoffIndex) {
      try {
        LOGGER.error("Deleted orphaned snapshot {}", pendingSnapshot);
        FileUtil.deleteFolder(pendingSnapshot);
        LOGGER.debug("Deleted orphaned snapshot {}", pendingSnapshot);
      } catch (final IOException e) {
        LOGGER.warn(
            "Failed to delete orphaned snapshot {}, risk using unnecessary disk space",
            pendingSnapshot);
      }
    }
  }

  public Path getPath() {
    return snapshotsDirectory;
  }

  @Override
  public void close() {
    // nothing to be done
  }

  public Snapshot newSnapshot(
      final long index, final long term, final WallClockTimestamp timestamp, final Path directory) {
    return newSnapshot(new DirBasedSnapshotMetadata(index, term, timestamp), directory);
  }

  public Snapshot newSnapshot(final DirBasedSnapshotMetadata metadata, final Path directory) {

    if (currentSnapshot != null && currentSnapshot.id().compareTo(metadata) >= 0) {
      LOGGER.debug("Snapshot is older then {} already exists", currentSnapshot);
      return currentSnapshot;
    }

    final var destination = buildSnapshotDirectory(metadata);
    try {
      tryAtomicDirectoryMove(directory, destination);
    } catch (final FileAlreadyExistsException e) {
      LOGGER.debug(
          "Expected to move snapshot from {} to {}, but it already exists",
          directory,
          destination,
          e);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    final var previousSnapshot = currentSnapshot;

    currentSnapshot = new DirBasedSnapshot(destination, metadata);
    snapshotMetrics.incrementSnapshotCount();
    observeSnapshotSize(currentSnapshot);

    LOGGER.debug("Purging snapshots older than {}", currentSnapshot);
    if (previousSnapshot != null) {
      LOGGER.error("Deleting snapshot {}", previousSnapshot);
      previousSnapshot.delete();
    }
    purgePendingSnapshots(currentSnapshot.index());

    listeners.forEach(listener -> listener.onNewSnapshot(currentSnapshot));

    LOGGER.debug("Created new snapshot {}", currentSnapshot);
    return currentSnapshot;
  }
  //
  //  private DirBasedSnapshot put(final DirBasedSnapshot snapshot) {
  //    // caveat: if the metadata is the same but the location is different, this will do nothing
  //    final var previous = snapshots.put(snapshot.getMetadata(), snapshot);
  //    if (previous == null) {
  //      listeners.forEach(listener -> listener.onNewSnapshot(snapshot));
  //    }
  //
  //    LOGGER.debug("Committed new snapshot {}", snapshot);
  //    return snapshot;
  //  }
  //
  //  private DirBasedSnapshot put(final Path directory, final DirBasedSnapshotMetadata metadata) {
  //    if (snapshots.containsKey(metadata)) {
  //      LOGGER.debug("Snapshot {} already exists", metadata);
  //      return snapshots.get(metadata);
  //    }
  //
  //    final var destination = buildSnapshotDirectory(metadata);
  //    try {
  //      tryAtomicDirectoryMove(directory, destination);
  //    } catch (final FileAlreadyExistsException e) {
  //      LOGGER.debug(
  //          "Expected to move snapshot from {} to {}, but it already exists",
  //          directory,
  //          destination,
  //          e);
  //    } catch (final IOException e) {
  //      throw new UncheckedIOException(e);
  //    }
  //
  //    return put(new DirBasedSnapshot(destination, metadata));
  //  }

  private void purgePendingSnapshot(final Path pendingSnapshot) {
    try {
      FileUtil.deleteFolder(pendingSnapshot);
      LOGGER.error("Delete not completed (orphaned) snapshot {}", pendingSnapshot);
    } catch (final IOException e) {
      LOGGER.error("Failed to delete not completed (orphaned) snapshot {}", pendingSnapshot);
    }
  }

  private void tryAtomicDirectoryMove(final Path directory, final Path destination)
      throws IOException {
    try {
      Files.move(directory, destination, StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(directory, destination);
    }
  }

  private Path buildPendingSnapshotDirectory(
      final long index, final long term, final WallClockTimestamp timestamp) {
    final var metadata = new DirBasedSnapshotMetadata(index, term, timestamp);
    return pendingDirectory.resolve(metadata.getFileName());
  }

  private Path buildSnapshotDirectory(final DirBasedSnapshotMetadata metadata) {
    return snapshotsDirectory.resolve(metadata.getFileName());
  }
}
