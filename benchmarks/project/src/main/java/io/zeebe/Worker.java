/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.worker.JobWorker;
import io.zeebe.config.AppCfg;
import io.zeebe.config.WorkerCfg;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class Worker extends App {

  private final AppCfg appCfg;

  Worker(AppCfg appCfg) {
    this.appCfg = appCfg;
  }

  @Override
  public void run() {
    final WorkerCfg workerCfg = appCfg.getWorker();
    final String jobType = workerCfg.getJobType();
    final long completionDelay = workerCfg.getCompletionDelay().toMillis();
    final BlockingQueue<Future<?>> requestFutures = new ArrayBlockingQueue<>(10_000);

    final ZeebeClient client = createZeebeClient();
    printTopology(client);

    final JobWorker worker =
        client
            .newWorker()
            .jobType(jobType)
            .handler(
                (jobClient, job) -> {
                  try {
                    Thread.sleep(completionDelay);
                  } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted during completion delay, most likely shutting down", e);
                    return;
                  }

                  requestFutures.add(
                      jobClient
                          .newCompleteCommand(job.getKey())
                          .variables(job.getVariables())
                          .send());
                })
            .open();

    final ResponseChecker responseChecker = new ResponseChecker(requestFutures);
    responseChecker.start();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  worker.close();
                  client.close();
                  responseChecker.close();
                }));
  }

  private ZeebeClient createZeebeClient() {
    final WorkerCfg workerCfg = appCfg.getWorker();
    return ZeebeClient.newClientBuilder()
        .brokerContactPoint(appCfg.getBrokerUrl())
        .numJobWorkerExecutionThreads(workerCfg.getThreads())
        .defaultJobWorkerName(workerCfg.getWorkerName())
        .defaultJobTimeout(workerCfg.getCompletionDelay().multipliedBy(6))
        .defaultJobWorkerMaxJobsActive(workerCfg.getCapacity())
        .defaultJobPollInterval(workerCfg.getPollingDelay())
        .withProperties(System.getProperties())
        .build();
  }

  public static void main(String[] args) {
    createApp(Worker::new);
  }
}
