/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import io.atomix.raft.impl.zeebe.snapshot.AtomixRecordEntrySupplier;
import io.atomix.raft.snapshot.Snapshot;
import io.atomix.raft.snapshot.SnapshotStore;
import io.atomix.raft.snapshot.TransientSnapshot;
import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.broker.system.partitions.SnapshotReplication;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;

/** Controls how snapshot/recovery operations are performed */
public class StateSnapshotController implements SnapshotController {

  private static final Logger LOG = Loggers.SNAPSHOT_LOGGER;

  private final SnapshotStore store;
  private final ZeebeDbFactory zeebeDbFactory;
  private final ToLongFunction<ZeebeDb> exporterPositionSupplier;

  private final Path runtimeDirectory;
  private ZeebeDb db;
  private final AtomixRecordEntrySupplier entrySupplier;
  private final ReplicationController replicationController;

  public StateSnapshotController(
      final int partitionId,
      final ZeebeDbFactory zeebeDbFactory,
      final SnapshotStore store,
      final Path runtimeDirectory,
      final SnapshotReplication replication,
      final AtomixRecordEntrySupplier entrySupplier,
      final ToLongFunction<ZeebeDb> exporterPositionSupplier) {
    this.store = store;
    this.runtimeDirectory = runtimeDirectory;
    this.zeebeDbFactory = zeebeDbFactory;
    this.exporterPositionSupplier = exporterPositionSupplier;
    this.entrySupplier = entrySupplier;
    replicationController = new ReplicationController(partitionId, replication, store);
    store.addSnapshotListener(replicationController);
  }

  @Override
  public Optional<TransientSnapshot> takeTempSnapshot(final long lowerBoundSnapshotPosition) {
    if (!isDbOpened()) {
      return Optional.empty();
    }

    final long exportedPosition = exporterPositionSupplier.applyAsLong(openDb());
    final long snapshotPosition = Math.min(exportedPosition, lowerBoundSnapshotPosition);

    final var optionalIndexed = entrySupplier.getIndexedEntry(snapshotPosition);

    final Long previousSnapshotIndex =
        store.getLatestSnapshot().map(Snapshot::getCompactionBound).orElse(-1L);

    final var optTransientSnapshot =
        optionalIndexed
            .filter(indexed -> indexed.index() != previousSnapshotIndex)
            .map(
                indexed ->
                    store.takeTransientSnapshot(
                        indexed.index(),
                        indexed.entry().term(),
                        WallClockTimestamp.from(System.currentTimeMillis())));

    optTransientSnapshot.ifPresent(this::createSnapshot);
    return optTransientSnapshot;
  }

  @Override
  public void consumeReplicatedSnapshots() {
    replicationController.consumeReplicatedSnapshots();
  }

  @Override
  public void recover() throws Exception {
    final var runtimeDirectory = getRuntimeDirectory();

    if (Files.exists(runtimeDirectory)) {
      FileUtil.deleteFolder(runtimeDirectory);
    }

    final var optLatestSnapshot = store.getLatestSnapshot();
    if (optLatestSnapshot.isPresent()) {
      final var snapshot = optLatestSnapshot.get();
      LOG.debug("Available snapshot: {}", snapshot);

      FileUtil.copySnapshot(runtimeDirectory, snapshot.getPath());

      try {
        // open database to verify that the snapshot is recoverable
        openDb();
        LOG.debug("Recovered state from snapshot '{}'", snapshot);
      } catch (final Exception e) {
        FileUtil.deleteFolder(runtimeDirectory);

        LOG.error(
            "Failed to open snapshot '{}'. No snapshots available to recover from. Manual action is required.",
            snapshot,
            e);
        throw new IllegalStateException("Failed to recover from snapshots", e);
      }
    }
  }

  @Override
  public ZeebeDb openDb() {
    if (db == null) {
      final var runtimeDirectory = getRuntimeDirectory();
      db = zeebeDbFactory.createDb(runtimeDirectory.toFile());
      LOG.debug("Opened database from '{}'.", runtimeDirectory);
    }

    return db;
  }

  @Override
  public int getValidSnapshotsCount() {
    return store.getLatestSnapshot().isPresent() ? 1 : 0;
  }

  // todo(zell): only used for tests.
  @Override
  public File getLastValidSnapshotDirectory() {
    return store.getLatestSnapshot().map(Snapshot::getPath).map(Path::toFile).orElse(null);
  }

  Path getRuntimeDirectory() {
    return runtimeDirectory;
  }

  @Override
  public void close() throws Exception {
    if (db != null) {
      db.close();
      LOG.debug("Closed database from '{}'.", runtimeDirectory);
      db = null;
    }
  }

  boolean isDbOpened() {
    return db != null;
  }

  private boolean createSnapshot(final TransientSnapshot snapshot) {
    //    final var snapshotDir = snapshot.getPath();
    //    final var start = System.currentTimeMillis();
    //
    //    if (db == null) {
    //      LOG.error("Expected to take a snapshot, but no database was opened");
    //      return false;
    //    }

    snapshot.take(
        snapshotDir -> {
          if (db == null) {
            LOG.error("Expected to take a snapshot, but no database was opened");
            return false;
          }

          LOG.debug("Taking temporary snapshot into {}.", snapshotDir);
          try {
            db.createSnapshot(snapshotDir.toFile());
          } catch (final Exception e) {
            LOG.error("Failed to create snapshot of runtime database", e);
            return false;
          }

          return true;
        });
    //
    //    LOG.debug("Taking temporary snapshot into {}.", snapshotDir);
    //    try {
    //      db.createSnapshot(snapshotDir.toFile());
    //    } catch (final Exception e) {
    //      LOG.error("Failed to create snapshot of runtime database", e);
    //      return false;
    //    }

    //    final var elapsedSeconds = System.currentTimeMillis() - start;
    //    storage.getMetrics().observeSnapshotOperation(elapsedSeconds);

    return true;
  }
}
