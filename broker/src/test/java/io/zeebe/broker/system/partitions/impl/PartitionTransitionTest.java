/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.PartitionContext;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import java.util.Collections;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

public class PartitionTransitionTest {

  @Rule
  public final ControlledActorSchedulerRule schedulerRule = new ControlledActorSchedulerRule();

  private PartitionContext ctx;

  @Before
  public void setup() {
    ctx = mock(PartitionContext.class);
    when(ctx.getPartitionId()).thenReturn(0);
  }

  @Test
  public void shouldCloseInOppositeOrderOfOpen() {
    // given
    final NoopComponent firstComponent = spy(new NoopComponent());
    final NoopComponent secondComponent = spy(new NoopComponent());
    final PartitionTransitionImpl partitionTransition =
        new PartitionTransitionImpl(
            ctx, List.of(firstComponent, secondComponent), Collections.EMPTY_LIST);

    // when
    final TestActor actor =
        new TestActor(
            () -> {
              final CompletableActorFuture<Void> leaderFuture = new CompletableActorFuture<>();
              partitionTransition.toLeader(leaderFuture);
              leaderFuture.onComplete(
                  (nothing, err) -> {
                    Assertions.assertThat(err).isNull();

                    final CompletableActorFuture<Void> closeFuture = new CompletableActorFuture<>();
                    partitionTransition.toInactive(closeFuture);
                    closeFuture.onComplete(
                        (nothing1, err1) -> Assertions.assertThat(err1).isNull());
                  });
            });

    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // then
    final InOrder order = inOrder(firstComponent, secondComponent);
    order.verify(firstComponent).open(ctx);
    order.verify(secondComponent).open(ctx);
    order.verify(secondComponent).close(ctx);
    order.verify(firstComponent).close(ctx);
  }

  private static final class TestActor extends Actor {
    private final Runnable runnable;

    private TestActor(final Runnable runnable) {
      super();
      this.runnable = runnable;
    }

    @Override
    protected void onActorStarted() {
      super.onActorStarted();
      runnable.run();
    }
  }

  private static class NoopComponent implements Component<Void> {

    @Override
    public ActorFuture<Void> open(final PartitionContext context) {
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> close(final PartitionContext context) {
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> onOpen(final PartitionContext context, final Void aVoid) {
      return CompletableActorFuture.completed(null);
    }

    @Override
    public String getName() {
      return "NoopComponent";
    }
  }
}
