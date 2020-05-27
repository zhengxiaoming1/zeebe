/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.logstreams;

import io.atomix.raft.snapshot.Snapshot;
import io.atomix.raft.snapshot.SnapshotListener;
import io.atomix.raft.snapshot.SnapshotStore;
import io.zeebe.broker.Loggers;
import io.zeebe.util.sched.Actor;

public final class LogDeletionService extends Actor implements SnapshotListener {
  private final LogCompactor logCompactor;
  private final String actorName;
  private final SnapshotStore snapshotStore;

  public LogDeletionService(
      final int nodeId,
      final int partitionId,
      final LogCompactor logCompactor,
      final SnapshotStore snapshotStore) {
    this.snapshotStore = snapshotStore;
    this.logCompactor = logCompactor;
    actorName = buildActorName(nodeId, "DeletionService-" + partitionId);
  }

  @Override
  public String getName() {
    return actorName;
  }

  @Override
  protected void onActorStarting() {
    snapshotStore.addSnapshotListener(this);
  }

  @Override
  protected void onActorClosing() {
    if (snapshotStore != null) {
      snapshotStore.removeSnapshotListener(this);
    }
  }

  @Override
  public void onNewSnapshot(final Snapshot newSnapshot) {
    actor.run(() -> delegateDeletion(newSnapshot));
  }

  private void delegateDeletion(final Snapshot snapshot) {
    final var compactionBound = snapshot.getCompactionBound();
    logCompactor
        .compactLog(compactionBound)
        .exceptionally(error -> logCompactionError(compactionBound, error))
        .join();
  }

  private Void logCompactionError(final long compactionBound, final Throwable error) {
    if (error != null) {
      Loggers.DELETION_SERVICE.error(
          "Failed to compact Atomix log up to index {}", compactionBound, error);
    }

    return null;
  }
}
