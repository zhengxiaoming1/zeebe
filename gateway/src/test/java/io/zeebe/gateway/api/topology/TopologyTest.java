/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.topology;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.api.util.GatewayTest;
import io.zeebe.gateway.impl.broker.cluster.BrokerClusterStateImpl;
import io.zeebe.gateway.protocol.GatewayOuterClass.BrokerInfo;
import io.zeebe.gateway.protocol.GatewayOuterClass.Partition.PartitionBrokerHealth;
import io.zeebe.gateway.protocol.GatewayOuterClass.TopologyRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.TopologyResponse;
import org.junit.Test;

public class TopologyTest extends GatewayTest {

  @Test
  public void shouldResponseWithInitialUnhealthyPartitions() {
    // when
    final TopologyResponse topologyResponse = client.topology(TopologyRequest.newBuilder().build());

    // then
    assertThat(topologyResponse.getBrokersList())
        .isNotEmpty()
        .allSatisfy(
            brokerInfo ->
                assertThat(brokerInfo.getPartitionsList())
                    .isNotEmpty()
                    .allSatisfy(
                        partitionPerBroker ->
                            assertThat(partitionPerBroker.getHealth())
                                .isEqualTo(PartitionBrokerHealth.UNHEALTHY)));
  }

  @Test
  public void shouldUpdatePartitionHealthHealthy() {
    // given
    final BrokerClusterStateImpl topology =
        (BrokerClusterStateImpl) brokerClient.getTopologyManager().getTopology();
    topology.setPartitionHealthy(0, 1);

    // when
    final TopologyResponse topologyResponse = client.topology(TopologyRequest.newBuilder().build());

    // then
    final PartitionBrokerHealth health =
        topologyResponse.getBrokers(0).getPartitions(0).getHealth();
    assertThat(health).isEqualTo(PartitionBrokerHealth.HEALTHY);
  }

  @Test
  public void shouldUpdatePartitionHealthUnhealthy() {
    // given
    final BrokerClusterStateImpl topology =
        (BrokerClusterStateImpl) brokerClient.getTopologyManager().getTopology();
    topology.setPartitionHealthy(0, 1);
    final BrokerClusterStateImpl topologyAfterUpdate =
        (BrokerClusterStateImpl) brokerClient.getTopologyManager().getTopology();
    topologyAfterUpdate.setPartitionUnhealthy(0, 1);
    topologyAfterUpdate.setPartitionHealthy(0, 6);

    // when
    final TopologyResponse responseAfterUpdate =
        client.topology(TopologyRequest.newBuilder().build());

    // then
    final BrokerInfo brokerInfo = responseAfterUpdate.getBrokers(0);
    assertThat(brokerInfo.getPartitions(0).getHealth()).isEqualTo(PartitionBrokerHealth.UNHEALTHY);
    assertThat(brokerInfo.getPartitions(5).getHealth()).isEqualTo(PartitionBrokerHealth.HEALTHY);
  }
}
