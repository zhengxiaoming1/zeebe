/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe;

import io.zeebe.config.AppCfg;
import java.util.Arrays;
import java.util.List;

public class AllInOne extends App {

  private final AppCfg appCfg;

  private AllInOne(AppCfg appCfg) {
    this.appCfg = appCfg;
  }

  @Override
  public void run() {
    final List<Thread> threads =
        Arrays.asList(new Thread(new Starter(appCfg)), new Thread(new Worker(appCfg)));

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Thread was interrupted, most likely shutting down...", e);
      }
    }
  }

  public static void main(String[] args) {
    createApp(AllInOne::new);
  }
}
