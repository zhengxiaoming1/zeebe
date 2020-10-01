/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import io.zeebe.util.sched.future.CompletableActorFuture;

public interface PartitionTransition {

  /**
   * Transitions to follower asynchronously by closing the current partition's components and
   * opening a follower partition.
   *
   * @param future completed when the transition is complete
   */
  void toFollower(CompletableActorFuture<Void> future);

  /**
   * Transitions to leader asynchronously by closing the current partition's components and opening
   * a leader partition.
   *
   * @param future completed when the transition is complete
   */
  void toLeader(CompletableActorFuture<Void> future);

  /**
   * Closes the current partition's components asynchronously.
   *
   * @param future completed when the transition is complete
   */
  void toInactive(CompletableActorFuture<Void> future);
}
