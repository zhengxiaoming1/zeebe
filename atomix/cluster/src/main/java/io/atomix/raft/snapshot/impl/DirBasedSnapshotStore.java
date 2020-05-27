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

import io.atomix.raft.impl.zeebe.snapshot.DbSnapshotId;
import io.atomix.raft.impl.zeebe.snapshot.DbSnapshotMetadata;
import io.atomix.raft.impl.zeebe.snapshot.SnapshotMetrics;
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

// import io.atomix.raft.impl.zeebe.snapshot.SnapshotImpl;

public final class DirBasedSnapshotStore implements SnapshotStore {

  private static final Logger LOGGER = new ZbLogger(DirBasedSnapshotStore.class);

  // if thread-safe is a must, then switch to ConcurrentNavigableMap
  // a map of all available snapshots indexed by index
  private Snapshot currentSnapshot = null;

  // the root snapshotsDirectory where all snapshots should be stored
  private final Path snapshotsDirectory;
  // the root snapshotsDirectory when pending snapshots should be stored
  private final Path pendingDirectory;
  // keeps track of all snapshot modification listeners
  private final Set<SnapshotListener> listeners;
  // a pair of mutable snapshot ID for index-only lookups
  private final ReusableSnapshotId lowerBoundId;
  private final ReusableSnapshotId upperBoundId;

  private final SnapshotMetrics snapshotMetrics;

  public DirBasedSnapshotStore(
      final SnapshotMetrics snapshotMetrics,
      final Path snapshotsDirectory,
      final Path pendingDirectory) {
    this.snapshotsDirectory = snapshotsDirectory;
    this.pendingDirectory = pendingDirectory;
    this.snapshotMetrics = snapshotMetrics;

    this.lowerBoundId = new ReusableSnapshotId(WallClockTimestamp.from(0));
    this.upperBoundId = new ReusableSnapshotId(WallClockTimestamp.from(Long.MAX_VALUE));
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
  // todo(zell) do we really want that?
  //  @Override
  //  public SnapshotMetrics getMetrics() {
  //    return metrics;
  //  }

  //  @Override
  //  public void onNewSnapshot(
  //      final io.atomix.raft.storage.snapshot.Snapshot snapshot, final SnapshotStore store) {
  //    metrics.incrementSnapshotCount();
  //    observeSnapshotSize(snapshot);
  //
  //    LOGGER.debug("Purging snapshots older than {}", snapshot);
  //    store.purgeSnapshots(snapshot);
  //    purgePendingSnapshots(snapshot.index());
  //
  //    final var optionalConverted = toSnapshot(snapshot.getPath());
  //    if (optionalConverted.isPresent()) {
  //      final var converted = optionalConverted.get();
  //      // TODO #4067(@korthout): rename onSnapshotsDeleted, because it doesn't always delete
  //      deletionListeners.forEach(listener -> listener.onSnapshotsDeleted(converted));
  //    }
  //  }

  //  @Override
  //  public void onSnapshotDeletion(
  //      final io.atomix.raft.storage.snapshot.Snapshot snapshot, final SnapshotStore store) {
  //    metrics.decrementSnapshotCount();
  //    LOGGER.debug("Snapshot {} removed from store {}", snapshot, store);
  //  }

  @Override
  public TransientSnapshot takeTransientSnapshot(
      final long index, final long term, final WallClockTimestamp timestamp) {
    final var directory = buildPendingSnapshotDirectory(index, term, timestamp);
    return new DirBasedTransientSnapshot(index, term, timestamp, directory, this);
  }

  @Override
  public Optional<Snapshot> getLatestSnapshot() {
    return Optional.ofNullable(currentSnapshot);
  }

  //
  //  @Override
  //  public void purgeSnapshots(final Snapshot snapshot) {
  //    if (!(snapshot instanceof DbSnapshot)) {
  //      throw new IllegalArgumentException(
  //          String.format(
  //              "Expected purge request with known DbSnapshot, but receive '%s'",
  //              snapshot.getClass()));
  //    }
  //
  //    final DbSnapshot dbSnapshot = (DbSnapshot) snapshot;
  //    snapshots.headMap(dbSnapshot.getMetadata(), false).values().forEach(this::remove);
  //  }
  //
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
  public void delete() {
    // currently only called by Atomix when permanently leaving a cluster - it should be safe here
    // to not update the metrics, as they will simply disappear as time moves on. Once we have a
    // single store/replication mechanism, we can consider updating the metrics here
    currentSnapshot = null;
    //    snapshotMetrics.decrementSnapshotCount();

    try {
      FileUtil.deleteFolder(snapshotsDirectory);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    try {
      FileUtil.deleteFolder(pendingDirectory);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  //
  //  private Optional<io.atomix.raft.impl.zeebe.snapshot.Snapshot> createNewCommittedSnapshot(
  //      final Path snapshotPath, final DbSnapshotMetadata metadata) {
  //    try (final var created =
  //        store.newSnapshot(
  //            metadata.getIndex(), metadata.getTerm(), metadata.getTimestamp(), snapshotPath)) {
  //      return Optional.of(new SnapshotImpl(metadata.getIndex(), created.getPath()));
  //    } catch (final UncheckedIOException e) {
  //      LOGGER.error("Failed to commit pending snapshot {} located at {}", metadata, snapshotPath,
  // e);
  //      return Optional.empty();
  //    }
  //  }

  //  private Path getPendingDirectoryFor(final DbSnapshotMetadata metadata) {
  //    return pendingDirectory.resolve(metadata.getFileName());
  //  }
  //
  //  private Path getPendingDirectoryFor(final Indexed<? extends RaftLogEntry> entry) {
  //    final var metadata =
  //        new DbSnapshotMetadata(
  //            entry.index(),
  //            entry.entry().term(),
  //            WallClockTimestamp.from(System.currentTimeMillis()));
  //    return getPendingDirectoryFor(metadata);
  //  }
  //
  @Override
  public long getCurrentSnapshotIndex() {
    return getLatestSnapshot().map(Snapshot::index).orElse(0L);
  }
  //
  //  @Override
  //  public Snapshot getCurrentSnapshot() {
  //    return getLatestSnapshot().orElse(null);
  //  }
  //
  //  private Optional<io.atomix.raft.impl.zeebe.snapshot.Snapshot> toSnapshot(final Path path) {
  //    return DbSnapshotMetadata.ofPath(path)
  //        .map(metadata -> new SnapshotImpl(metadata.getIndex(), path));
  //  }
  //
  //  private io.atomix.raft.impl.zeebe.snapshot.Snapshot getSnapshot(
  //      final Indexed<? extends RaftLogEntry> indexed) {
  //    final var pending = getPendingDirectoryFor(indexed);
  //    return new SnapshotImpl(indexed.index(), pending);
  //  }

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
    final var optionalMetadata = DbSnapshotMetadata.ofPath(pendingSnapshot);
    if (optionalMetadata.isPresent() && optionalMetadata.get().getIndex() < cutoffIndex) {
      try {
        FileUtil.deleteFolder(pendingSnapshot);
        LOGGER.debug("Deleted orphaned snapshot {}", pendingSnapshot);
      } catch (final IOException e) {
        LOGGER.warn(
            "Failed to delete orphaned snapshot {}, risk using unnecessary disk space",
            pendingSnapshot);
      }
    }
  }
  //
  //  @Override
  public Path getPath() {
    return snapshotsDirectory;
  }
  //
  //  @Override
  //  public Collection<? extends Snapshot> getSnapshots() {
  //    return snapshots.values();
  //  }

  /**
   * Returns the newest snapshot for the given index, meaning the snapshot with the given index with
   * the highest timestamp.
   *
   * @param index index of the snapshot
   * @return a snapshot, or null if there are no known snapshots for this index
   */
  //  @Override
  //  public Snapshot getSnapshot(final long index) {
  //    // it's possible (though unlikely) to have more than one snapshot per index, so we fallback
  // to
  //    // the one with the highest timestamp
  //    final var indexBoundedSet =
  //        snapshots.subMap(lowerBoundId.setIndex(index), false, upperBoundId.setIndex(index),
  // false);
  //    if (indexBoundedSet.isEmpty()) {
  //      return null;
  //    }
  //
  //    return indexBoundedSet.lastEntry().getValue();
  //  }
  @Override
  public void close() {
    // nothing to be done
  }

  public Snapshot newSnapshot(
      final long index, final long term, final WallClockTimestamp timestamp, final Path directory) {

    final var metadata = new DirBasedSnapshotMetadata(index, term, timestamp);

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
      LOGGER.debug("Deleting snapshot {}", previousSnapshot);
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
      LOGGER.debug("Delete not completed (orphaned) snapshot {}", pendingSnapshot);
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

  //
  private void remove(final DirBasedSnapshot snapshot) {
    LOGGER.debug("Deleting snapshot {}", snapshot);
    snapshot.delete();
    //      snapshots.remove(snapshot.getMetadata());
    //      listeners.forEach(l -> l.onSnapshotDeletion(snapshot, this));
    //      LOGGER.trace("Snapshots count: {}", snapshots.size());
  }

  private Path buildPendingSnapshotDirectory(
      final long index, final long term, final WallClockTimestamp timestamp) {
    final var metadata = new DirBasedSnapshotMetadata(index, term, timestamp);
    return pendingDirectory.resolve(metadata.getFileName());
  }

  private Path buildSnapshotDirectory(final DirBasedSnapshotMetadata metadata) {
    return snapshotsDirectory.resolve(metadata.getFileName());
  }

  private static final class ReusableSnapshotId implements DbSnapshotId {

    private final WallClockTimestamp timestamp;
    private long index;

    private ReusableSnapshotId(final WallClockTimestamp position) {
      this.timestamp = position;
    }

    @Override
    public long getIndex() {
      return index;
    }

    @Override
    public WallClockTimestamp getTimestamp() {
      return timestamp;
    }

    private ReusableSnapshotId setIndex(final long index) {
      this.index = index;
      return this;
    }
  }
}
