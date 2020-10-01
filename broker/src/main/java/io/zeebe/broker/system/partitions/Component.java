/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import io.zeebe.util.sched.future.ActorFuture;

/**
 * A Component is responsible for managing the opening and closing of some service (e.g., logStream,
 * AsyncSnapshotDirector, etc) defined as its parameter.
 *
 * @param <T> the value opened and closed by the Component
 */
public interface Component<T> {

  /**
   * Opens the component and returns a future which should be completed with the component's value.
   *
   * @param context the partition context
   * @return future
   */
  ActorFuture<T> open(final PartitionContext context);

  /**
   * Closes the component and returns a future to be completed after the closing is done.
   *
   * @param context the partition context
   * @return future
   */
  ActorFuture<Void> close(final PartitionContext context);

  /**
   * Called with the result of {@link Component#open(PartitionContext)} as the 'value' parameter.
   * Normally used to perform actions like storing the value in the {@link PartitionContext},
   * scheduling actors, etc.
   *
   * @param context the partition context
   * @param value value used to complete {@link Component#open(PartitionContext)} successfuly
   * @return future
   */
  ActorFuture<Void> onOpen(final PartitionContext context, final T value);

  /** @return A log-friendly identification of the component. */
  String getName();
}
