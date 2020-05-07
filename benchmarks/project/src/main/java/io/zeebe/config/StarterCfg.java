/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.config;

public class StarterCfg {

  private String processId;
  private int rate;
  private int threads;

  /** Paths are relative to classpath. */
  private String bpmnXmlPath;

  private String payloadPath;

  public String getProcessId() {
    return processId;
  }

  public void setProcessId(String processId) {
    this.processId = processId;
  }

  public int getRate() {
    return rate;
  }

  public void setRate(int rate) {
    this.rate = rate;
  }

  public int getThreads() {
    return threads;
  }

  public void setThreads(int threads) {
    this.threads = threads;
  }

  public String getBpmnXmlPath() {
    return bpmnXmlPath;
  }

  public void setBpmnXmlPath(String bpmnXmlPath) {
    this.bpmnXmlPath = bpmnXmlPath;
  }

  public String getPayloadPath() {
    return payloadPath;
  }

  public void setPayloadPath(String payloadPath) {
    this.payloadPath = payloadPath;
  }
}
