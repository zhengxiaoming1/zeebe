/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.it.GrpcClientRule;
import io.zeebe.client.api.response.BrokerInfo;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.WorkflowInstanceCreationIntent;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class AvailabilityTest {

  private static final String JOBTYPE = "availability-test";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent()
          .serviceTask(
              "task",
              t -> {
                t.zeebeTaskType(JOBTYPE);
              })
          .endEvent()
          .done();
  public Timeout testTimeout = Timeout.seconds(120);
  private final int partitionCount = 3;
  public ClusteringRule clusteringRule = new ClusteringRule(partitionCount, 1, 3);
  public GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  private long workflowKey;

  @Before
  public void setup() {
    workflowKey = clientRule.deployWorkflow(WORKFLOW);
  }

  @Test
  public void shouldCreateWorkflowWhenOnePartitionDown() {
    final BrokerInfo leaderForPartition = clusteringRule.getLeaderForPartition(partitionCount);
    clusteringRule.stopBroker(leaderForPartition.getNodeId(), false);

    for (int i = 0; i < 2 * partitionCount; i++) {
      clientRule.createWorkflowInstance(workflowKey);
    }

    final List<Integer> partitionIds =
        RecordingExporter.workflowInstanceCreationRecords()
            .withIntent(WorkflowInstanceCreationIntent.CREATED)
            .map(Record::getPartitionId)
            .limit(2 * partitionCount)
            .collect(Collectors.toList());

    assertThat(partitionIds.stream().filter(p -> p == 1).count()).isEqualTo(3);
    assertThat(partitionIds.stream().filter(p -> p == 2).count()).isEqualTo(3);
  }
}
