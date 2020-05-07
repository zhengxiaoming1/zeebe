/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.config;

public class AppCfg {

  private String brokerUrl;
  private StarterCfg starter;
  private WorkerCfg worker;

  public String getBrokerUrl() {
    return brokerUrl;
  }

  public void setBrokerUrl(String brokerUrl) {
    this.brokerUrl = brokerUrl;
  }

  public StarterCfg getStarter() {
    return starter;
  }

  public void setStarter(StarterCfg starter) {
    this.starter = starter;
  }

  public WorkerCfg getWorker() {
    return worker;
  }

  public void setWorker(WorkerCfg worker) {
    this.worker = worker;
  }
}
