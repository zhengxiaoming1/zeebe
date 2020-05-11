/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.job;

import io.zeebe.gateway.Loggers;
import io.zeebe.gateway.impl.broker.BrokerClient;
import io.zeebe.gateway.impl.broker.BrokerResponseConsumer;
import io.zeebe.gateway.impl.broker.RoundRobinDispatchStrategy;
import io.zeebe.gateway.impl.broker.cluster.BrokerTopologyManager;
import io.zeebe.gateway.impl.broker.request.BrokerCreateWorkflowInstanceRequest;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceCreationRecord;
import java.util.function.Consumer;

public class CreateWorkflowHandler {

  private final BrokerClient brokerClient;
  private final BrokerTopologyManager topologyManager;
  private final RoundRobinDispatchStrategy roundRobinDispatchStrategy;

  public CreateWorkflowHandler(
      final BrokerClient brokerClient, final BrokerTopologyManager topologyManager) {
    this.brokerClient = brokerClient;
    this.topologyManager = topologyManager;
    roundRobinDispatchStrategy = new RoundRobinDispatchStrategy(topologyManager);
  }

  public void createWorkflow(
      final int partitionsCount,
      final BrokerCreateWorkflowInstanceRequest request,
      final BrokerResponseConsumer<WorkflowInstanceCreationRecord> responseConsumer,
      final Consumer<Throwable> throwableConsumer) {
    createWorkflow(
        request, partitionIdIteratorForType(partitionsCount), responseConsumer, throwableConsumer);
  }

  private void createWorkflow(
      final BrokerCreateWorkflowInstanceRequest request,
      final PartitionIdIterator partitionIdIterator,
      final BrokerResponseConsumer<WorkflowInstanceCreationRecord> responseConsumer,
      final Consumer<Throwable> throwableConsumer) {

    if (partitionIdIterator.hasNext()) {
      final int partitionId = partitionIdIterator.next();

      // partitions to check
      request.setPartitionId(partitionId);
      brokerClient.sendRequest(
          request,
          responseConsumer,
          error -> {
            Loggers.GATEWAY_LOGGER.warn(
                "Failed to create workflow on partition {}",
                partitionIdIterator.getCurrentPartitionId(),
                error);
            createWorkflow(request, partitionIdIterator, responseConsumer, throwableConsumer);
          },
          response -> false);
    } else {
      // no partition left to check
      throwableConsumer.accept(new RuntimeException("Partition not found"));
    }
  }

  private PartitionIdIterator partitionIdIteratorForType(final int partitionsCount) {
    final int nextPartitionId = roundRobinDispatchStrategy.determinePartition();
    return new PartitionIdIterator(nextPartitionId, partitionsCount, topologyManager);
  }
}
