/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.config;

import java.time.Duration;

public class WorkerCfg {

  private String jobType;
  private String workerName;

  private int threads;

  private int capacity;
  private Duration pollingDelay;
  private Duration completionDelay;

  public String getJobType() {
    return jobType;
  }

  public void setJobType(String jobType) {
    this.jobType = jobType;
  }

  public String getWorkerName() {
    return workerName;
  }

  public void setWorkerName(String workerName) {
    this.workerName = workerName;
  }

  public int getThreads() {
    return threads;
  }

  public void setThreads(int threads) {
    this.threads = threads;
  }

  public int getCapacity() {
    return capacity;
  }

  public void setCapacity(int capacity) {
    this.capacity = capacity;
  }

  public Duration getPollingDelay() {
    return pollingDelay;
  }

  public void setPollingDelay(Duration pollingDelay) {
    this.pollingDelay = pollingDelay;
  }

  public Duration getCompletionDelay() {
    return completionDelay;
  }

  public void setCompletionDelay(Duration completionDelay) {
    this.completionDelay = completionDelay;
  }
}
