/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import io.atomix.raft.snapshot.Snapshot;
import io.atomix.raft.snapshot.SnapshotChunk;
import io.atomix.raft.snapshot.SnapshotListener;
import io.atomix.raft.snapshot.SnapshotStore;
import io.atomix.raft.snapshot.TransientSnapshot;
import io.zeebe.broker.system.partitions.SnapshotReplication;
import io.zeebe.util.ZbLogger;
import java.io.IOException;
import java.util.Map;
import org.agrona.collections.Object2NullableObjectHashMap;
import org.slf4j.Logger;

public final class ReplicationController implements SnapshotListener {
  private static final Logger LOG = new ZbLogger("io.atomix.raft.impl.zeebe.snapshot");
  private static final ReplicationContext INVALID_SNAPSHOT = new ReplicationContext(-1, null);

  private final SnapshotReplication replication;
  private final Map<String, ReplicationContext> receivedSnapshots =
      new Object2NullableObjectHashMap<>();
  private final SnapshotStore store;
  private final SnapshotReplicationMetrics metrics;

  public ReplicationController(
      final int partition, final SnapshotReplication replication, final SnapshotStore store) {
    this.replication = replication;
    this.store = store;
    this.metrics = new SnapshotReplicationMetrics(Integer.toString(partition));
  }

  @Override
  public void onNewSnapshot(final Snapshot newSnapshot) {
    final var snapshotChunkReader = newSnapshot.newChunkReader();

    while (snapshotChunkReader.hasNext()) {
      final var snapshotChunk = snapshotChunkReader.next();
      replication.replicate(snapshotChunk);
    }
  }

  /** Registering for consuming snapshot chunks. */
  public void consumeReplicatedSnapshots() {
    replication.consume(this::consumeSnapshotChunk);
  }

  /**
   * This is called by the snapshot replication implementation on each snapshot chunk
   *
   * @param snapshotChunk the chunk to consume
   */
  private void consumeSnapshotChunk(final SnapshotChunk snapshotChunk) {
    final String snapshotId = snapshotChunk.getSnapshotId();
    final String chunkName = snapshotChunk.getChunkName();

    final ReplicationContext context =
        receivedSnapshots.computeIfAbsent(
            snapshotId,
            id -> {
              final var startTimestamp = System.currentTimeMillis();
              final TransientSnapshot transientSnapshot =
                  store.takeTransientSnapshot(snapshotChunk.getSnapshotId());
              return newReplication(startTimestamp, transientSnapshot);
            });
    if (context == INVALID_SNAPSHOT) {
      LOG.trace(
          "Ignore snapshot chunk {}, because snapshot {} is marked as invalid.",
          chunkName,
          snapshotId);
      return;
    }

    final var transientSnapshot = context.getTransientSnapshot();
    try {
      if (transientSnapshot.write(snapshotChunk)) {
        validateWhenReceivedAllChunks(snapshotChunk, context);
      } else {
        markSnapshotAsInvalid(context, snapshotChunk);
      }
    } catch (final IOException e) {
      LOG.error("Unexepected error on writing the received snapshot chunk {}", snapshotChunk, e);
      markSnapshotAsInvalid(context, snapshotChunk);
    }
  }

  private void markSnapshotAsInvalid(
      final ReplicationContext replicationContext, final SnapshotChunk chunk) {
    replicationContext.getTransientSnapshot().abort();
    receivedSnapshots.put(chunk.getSnapshotId(), INVALID_SNAPSHOT);
    metrics.decrementCount();
  }

  private void validateWhenReceivedAllChunks(
      final SnapshotChunk snapshotChunk, final ReplicationContext context) {
    final int totalChunkCount = snapshotChunk.getTotalCount();

    if (context.incrementCount() == totalChunkCount) {
      LOG.debug(
          "Received all snapshot chunks ({}/{}), snapshot is valid",
          context.chunkCount,
          totalChunkCount);
      if (!tryToMarkSnapshotAsValid(snapshotChunk, context)) {
        LOG.debug("Failed to mark snapshot {} as valid", snapshotChunk.getSnapshotId());
      }
    } else {
      LOG.debug(
          "Waiting for more snapshot chunks, currently have {}/{}",
          context.chunkCount,
          totalChunkCount);
    }
  }

  private boolean tryToMarkSnapshotAsValid(
      final SnapshotChunk snapshotChunk, final ReplicationContext context) {

    // todo(zell): validate snapshot checksum
    // give it the commit thing?
    context.getTransientSnapshot().commit();

    //    if (snapshotConsumer.completeSnapshot(
    //        snapshotChunk.getSnapshotId(), snapshotChunk.getSnapshotChecksum())) {
    final var elapsed = System.currentTimeMillis() - context.getStartTimestamp();
    receivedSnapshots.remove(snapshotChunk.getSnapshotId());
    metrics.decrementCount();
    metrics.observeDuration(elapsed);

    return true;

    // todo(zell): check if verification works
    //      return true;
    //    } else {
    //      markSnapshotAsInvalid(context, snapshotChunk);
    //      return false;
    //    }
  }

  private ReplicationContext newReplication(
      final long startTimestamp, final TransientSnapshot transientSnapshot) {
    final var context = new ReplicationContext(startTimestamp, transientSnapshot);
    metrics.incrementCount();
    return context;
  }

  private static final class ReplicationContext {
    private final long startTimestamp;
    private final TransientSnapshot transientSnapshot;
    private long chunkCount;

    public ReplicationContext(
        final long startTimestamp, final TransientSnapshot transientSnapshot) {
      this.startTimestamp = startTimestamp;
      this.chunkCount = 0L;
      this.transientSnapshot = transientSnapshot;
    }

    public long getStartTimestamp() {
      return startTimestamp;
    }

    public long incrementCount() {
      return ++chunkCount;
    }

    public TransientSnapshot getTransientSnapshot() {
      return transientSnapshot;
    }
  }
}
