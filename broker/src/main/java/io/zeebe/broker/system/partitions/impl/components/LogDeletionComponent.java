/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl.components;

import io.zeebe.broker.logstreams.AtomixLogCompactor;
import io.zeebe.broker.logstreams.LogCompactor;
import io.zeebe.broker.logstreams.LogDeletionService;
import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.PartitionContext;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class LogDeletionComponent implements Component<LogDeletionService> {

  @Override
  public ActorFuture<LogDeletionService> open(final PartitionContext context) {
    final LogCompactor logCompactor =
        new AtomixLogCompactor(context.getRaftPartition().getServer());
    final LogDeletionService deletionService =
        new LogDeletionService(
            context.getNodeId(),
            context.getPartitionId(),
            logCompactor,
            context
                .getSnapshotStoreSupplier()
                .getPersistedSnapshotStore(context.getRaftPartition().name()));

    return CompletableActorFuture.completed(deletionService);
  }

  @Override
  public ActorFuture<Void> close(final PartitionContext context) {
    final ActorFuture<Void> future = context.getLogDeletionService().closeAsync();
    context.setLogDeletionService(null);
    return future;
  }

  @Override
  public ActorFuture<Void> onOpen(
      final PartitionContext context, final LogDeletionService deletionService) {
    context.setLogDeletionService(deletionService);
    return context.getScheduler().submitActor(deletionService);
  }

  @Override
  public String getName() {
    return "LogDeletionService";
  }
}
