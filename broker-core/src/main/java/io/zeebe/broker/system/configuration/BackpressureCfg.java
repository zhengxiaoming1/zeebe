/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

import io.zeebe.util.Environment;

public class BackpressureCfg implements ConfigurationEntry {

  private boolean enabled = true;
  private boolean useWindowed = true;
  private String algorithm = "vegas";

  @Override
  public void init(
      final BrokerCfg globalConfig, final String brokerBase, final Environment environment) {
    environment.getBool(EnvironmentConstants.ENV_BACKPRESSURE_ENABLED).ifPresent(this::setEnabled);
    environment
        .getBool(EnvironmentConstants.ENV_BACKPRESSURE_WINDOWED)
        .ifPresent(this::setUseWindowed);
    environment.get(EnvironmentConstants.ENV_BACKPRESSURE_ALGORITHM).ifPresent(this::setAlgorithm);
  }

  public boolean isEnabled() {
    return enabled;
  }

  public BackpressureCfg setEnabled(final boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  public boolean useWindowed() {
    return useWindowed;
  }

  public BackpressureCfg setUseWindowed(final boolean useWindowed) {
    this.useWindowed = useWindowed;
    return this;
  }

  public LimitAlgorithm getAlgorithm() {
    return LimitAlgorithm.valueOf(algorithm.toUpperCase());
  }

  public BackpressureCfg setAlgorithm(final String algorithm) {
    this.algorithm = algorithm;
    return this;
  }

  public enum LimitAlgorithm {
    VEGAS,
    GRADIENT,
    GRADIENT2
  }
}
