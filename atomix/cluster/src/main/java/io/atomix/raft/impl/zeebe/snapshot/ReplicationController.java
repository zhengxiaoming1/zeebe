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
/// *
// * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
// * one or more contributor license agreements. See the NOTICE file distributed
// * with this work for additional information regarding copyright ownership.
// * Licensed under the Zeebe Community License 1.0. You may not use this file
// * except in compliance with the Zeebe Community License 1.0.
// */
// package io.atomix.raft.impl.zeebe.snapshot;
//
// import io.atomix.raft.snapshot.Snapshot;
// import io.atomix.raft.snapshot.SnapshotListener;
// import io.atomix.raft.snapshot.SnapshotStore;
// import io.zeebe.util.ZbLogger;
// import java.io.File;
// import java.io.IOException;
// import java.util.Map;
// import org.agrona.collections.Object2NullableObjectHashMap;
// import org.slf4j.Logger;
//
// public final class ReplicationController implements SnapshotListener {
//  private static final Logger LOG = new ZbLogger("io.atomix.raft.impl.zeebe.snapshot");
//  private static final ReplicationContext INVALID_SNAPSHOT = new ReplicationContext(-1, -1);
//
//  private final SnapshotReplication replication;
//  private final Map<String, ReplicationContext> receivedSnapshots =
//      new Object2NullableObjectHashMap<>();
////  private final SnapshotReplicationMetrics metrics;
//
//  private final SnapshotConsumer snapshotConsumer;
//
//  public ReplicationController(
//      final SnapshotReplication replication, final SnapshotStore store) {
//    this.replication = replication;
//    this.snapshotConsumer = new FileSnapshotConsumer(store, LOG);
////    this.metrics = storage.getMetrics().getReplication();
//    this.metrics.setCount(0);
//  }
//
//  @Override
//  public void onNewSnapshot(final Snapshot newSnapshot) {
//    final var snapshotChunkReader = newSnapshot.newChunkReader();
//
//    while (snapshotChunkReader.hasNext())
//    {
//      final var snapshotChunk = snapshotChunkReader.next();
//
//      replicate(snapshotChunk.getSnapshotId(), snapshotChunk.getTotalCount(), snapshotChunk.);
//
//    }
////    for (SnapshotChunk chunk : newSnapshot.newChunkReader())
////    {
////
////    }
//
//  }
//
//  public void replicate(
//      final String snapshotId,
//      final int totalCount,
//      final File snapshotChunkFile,
//      final long snapshotChecksum) {
//    try {
//      final SnapshotChunk chunkToReplicate =
//          SnapshotChunkUtil.createSnapshotChunkFromFile(
//              snapshotChunkFile, snapshotId, totalCount, snapshotChecksum);
//      replication.replicate(chunkToReplicate);
//    } catch (final IOException ioe) {
//      LOG.error(
//          "Unexpected error on reading snapshot chunk from file '{}'.", snapshotChunkFile, ioe);
//    }
//  }
//
//  /** Registering for consuming snapshot chunks. */
//  public void consumeReplicatedSnapshots() {
//    replication.consume(this::consumeSnapshotChunk);
//  }
//
//  /**
//   * This is called by the snapshot replication implementation on each snapshot chunk
//   *
//   * @param snapshotChunk the chunk to consume
//   */
//  private void consumeSnapshotChunk(final SnapshotChunk snapshotChunk) {
//    final String snapshotId = snapshotChunk.getSnapshotId();
//    final String chunkName = snapshotChunk.getChunkName();
//
//    final ReplicationContext context =
//        receivedSnapshots.computeIfAbsent(snapshotId, this::newReplication);
//    if (context == INVALID_SNAPSHOT) {
//      LOG.trace(
//          "Ignore snapshot chunk {}, because snapshot {} is marked as invalid.",
//          chunkName,
//          snapshotId);
//      return;
//    }
//
//    if (snapshotConsumer.consumeSnapshotChunk(snapshotChunk)) {
//      validateWhenReceivedAllChunks(snapshotChunk, context);
//    } else {
//      markSnapshotAsInvalid(snapshotChunk);
//    }
//  }
//
//  private void markSnapshotAsInvalid(final SnapshotChunk chunk) {
//    snapshotConsumer.invalidateSnapshot(chunk.getSnapshotId());
//    receivedSnapshots.put(chunk.getSnapshotId(), INVALID_SNAPSHOT);
//    metrics.decrementCount();
//  }
//
//  private void validateWhenReceivedAllChunks(
//      final SnapshotChunk snapshotChunk, final ReplicationContext context) {
//    final int totalChunkCount = snapshotChunk.getTotalCount();
//    context.chunkCount++;
//
//    if (context.chunkCount == totalChunkCount) {
//      LOG.debug(
//          "Received all snapshot chunks ({}/{}), snapshot is valid",
//          context.chunkCount,
//          totalChunkCount);
//      if (!tryToMarkSnapshotAsValid(snapshotChunk, context)) {
//        LOG.debug("Failed to mark snapshot {} as valid", snapshotChunk.getSnapshotId());
//      }
//    } else {
//      LOG.debug(
//          "Waiting for more snapshot chunks, currently have {}/{}",
//          context.chunkCount,
//          totalChunkCount);
//    }
//  }
//
//  private boolean tryToMarkSnapshotAsValid(
//      final SnapshotChunk snapshotChunk, final ReplicationContext context) {
//    if (snapshotConsumer.completeSnapshot(
//        snapshotChunk.getSnapshotId(), snapshotChunk.getSnapshotChecksum())) {
//      final var elapsed = System.currentTimeMillis() - context.startTimestamp;
//      receivedSnapshots.remove(snapshotChunk.getSnapshotId());
//      metrics.decrementCount();
//      metrics.observeDuration(elapsed);
//
//      return true;
//    } else {
//      markSnapshotAsInvalid(snapshotChunk);
//      return false;
//    }
//  }
//
//  private ReplicationContext newReplication(final String ignored) {
//    final var context = new ReplicationContext(0L, System.currentTimeMillis());
//    metrics.incrementCount();
//    return context;
//  }
//
//  private static final class ReplicationContext {
//    private final long startTimestamp;
//    private long chunkCount;
//
//    private ReplicationContext(final long chunkCount, final long startTimestamp) {
//      this.chunkCount = chunkCount;
//      this.startTimestamp = startTimestamp;
//    }
//  }
// }
