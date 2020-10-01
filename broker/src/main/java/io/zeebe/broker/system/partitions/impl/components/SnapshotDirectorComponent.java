/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl.components;

import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.PartitionContext;
import io.zeebe.broker.system.partitions.impl.AsyncSnapshotDirector;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;

public class SnapshotDirectorComponent implements Component<AsyncSnapshotDirector> {

  @Override
  public ActorFuture<AsyncSnapshotDirector> open(final PartitionContext context) {
    final Duration snapshotPeriod = context.getBrokerCfg().getData().getSnapshotPeriod();
    final AsyncSnapshotDirector snapshotDirector =
        new AsyncSnapshotDirector(
            context.getNodeId(),
            context.getStreamProcessor(),
            context.getSnapshotController(),
            context.getLogStream(),
            snapshotPeriod);

    return CompletableActorFuture.completed(snapshotDirector);
  }

  @Override
  public ActorFuture<Void> close(final PartitionContext context) {
    final ActorFuture<Void> future = context.getSnapshotDirector().closeAsync();
    context.setSnapshotDirector(null);
    return future;
  }

  @Override
  public ActorFuture<Void> onOpen(
      final PartitionContext context, final AsyncSnapshotDirector asyncSnapshotDirector) {
    context.setSnapshotDirector(asyncSnapshotDirector);
    return context.getScheduler().submitActor(asyncSnapshotDirector);
  }

  @Override
  public String getName() {
    return "AsyncSnapshotDirector";
  }
}
