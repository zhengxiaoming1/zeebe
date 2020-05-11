/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.broker.backpressure;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.limit.AIMDLimit;
import io.zeebe.gateway.impl.configuration.BackpressureCfg;
import io.zeebe.gateway.impl.configuration.BackpressureCfg.LimitAlgorithm;
import io.zeebe.gateway.impl.configuration.GatewayCfg;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public final class LimitFactory {
  public Limit ofConfig(final GatewayCfg config) {
    final LimitAlgorithm algorithm = config.getBackpressure().getAlgorithm();

    switch (algorithm) {
      case AIMD:
        return newAimdLimit(config);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Expected one of the following algorithms [%s], but got %s",
                Arrays.toString(BackpressureCfg.LimitAlgorithm.values()), algorithm));
    }
  }

  private AIMDLimit newAimdLimit(final GatewayCfg config) {
    final Duration requestTimeout = config.getCluster().getRequestTimeout();
    return AIMDLimit.newBuilder()
        .initialLimit(1000)
        .timeout(requestTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .build();
  }
}
