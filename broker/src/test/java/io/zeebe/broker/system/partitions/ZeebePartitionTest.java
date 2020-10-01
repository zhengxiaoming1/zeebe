/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.atomix.primitive.partition.PartitionId;
import io.atomix.raft.RaftServer.Role;
import io.atomix.raft.partition.RaftPartition;
import io.zeebe.util.health.CriticalComponentsHealthMonitor;
import io.zeebe.util.sched.future.CompletableActorFuture;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ZeebePartitionTest {

  @Rule public ControlledActorSchedulerRule schedulerRule = new ControlledActorSchedulerRule();

  private PartitionContext ctx;
  private PartitionTransition transition;

  @Before
  public void setup() {
    ctx = mock(PartitionContext.class);
    transition = spy(new NoopTransition());

    final RaftPartition raftPartition = mock(RaftPartition.class);
    when(raftPartition.id()).thenReturn(new PartitionId("", 0));
    when(raftPartition.getRole()).thenReturn(Role.INACTIVE);

    final CriticalComponentsHealthMonitor healthMonitor =
        mock(CriticalComponentsHealthMonitor.class);

    when(ctx.getRaftPartition()).thenReturn(raftPartition);
    when(ctx.getComponentHealthMonitor()).thenReturn(healthMonitor);
  }

  @Test
  public void shouldInstallLeaderPartition() {
    // given
    final ZeebePartition partition = new ZeebePartition(ctx, transition);
    schedulerRule.submitActor(partition);

    // when
    partition.onNewRole(Role.LEADER, 1);
    schedulerRule.workUntilDone();

    // then
    verify(transition).toLeader(any());
  }

  private static class NoopTransition implements PartitionTransition {

    @Override
    public void toFollower(final CompletableActorFuture<Void> future) {
      future.complete(null);
    }

    @Override
    public void toLeader(final CompletableActorFuture<Void> future) {
      future.complete(null);
    }

    @Override
    public void toInactive(final CompletableActorFuture<Void> future) {
      future.complete(null);
    }
  }
}
