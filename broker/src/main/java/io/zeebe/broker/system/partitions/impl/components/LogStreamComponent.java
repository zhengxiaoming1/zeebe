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
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class LogStreamComponent implements Component<LogStream> {

  @Override
  public ActorFuture<LogStream> open(final PartitionContext context) {
    return LogStream.builder()
        .withLogStorage(context.getAtomixLogStorage())
        .withLogName("logstream-" + context.getRaftPartition().name())
        .withNodeId(context.getNodeId())
        .withPartitionId(context.getRaftPartition().id().id())
        .withMaxFragmentSize(context.getMaxFragmentSize())
        .withActorScheduler(context.getScheduler())
        .buildAsync();
  }

  @Override
  public ActorFuture<Void> close(final PartitionContext context) {
    context.getComponentHealthMonitor().removeComponent(context.getLogStream().getLogName());
    final ActorFuture<Void> future = context.getLogStream().closeAsync();
    context.setLogStream(null);
    return future;
  }

  @Override
  public ActorFuture<Void> onOpen(final PartitionContext context, final LogStream logStream) {
    context.setLogStream(logStream);

    if (context.getDeferredCommitPosition() > 0) {
      context.getLogStream().setCommitPosition(context.getDeferredCommitPosition());
      context.setDeferredCommitPosition(-1);
    }
    context.getComponentHealthMonitor().registerComponent(logStream.getLogName(), logStream);

    return CompletableActorFuture.completed(null);
  }

  @Override
  public String getName() {
    return "logstream";
  }
}
