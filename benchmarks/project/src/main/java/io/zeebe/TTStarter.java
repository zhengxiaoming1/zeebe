/*
 * Copyright © 2019 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.prometheus.client.Counter;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.response.WorkflowInstanceEvent;
import io.zeebe.client.api.worker.JobWorker;
import io.zeebe.config.AppCfg;
import io.zeebe.config.TTStarterCfg;
import io.zeebe.config.WorkerCfg;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TTStarter extends App {
  private final AppCfg appCfg;
  private final BlockingQueue<ZeebeFuture<WorkflowInstanceEvent>> requestFutures;
  private ScheduledExecutorService executorService;
  private ScheduledExecutorService responseCheckExecutor;
  private final Map<Long, Long> runningWorkflows = new ConcurrentHashMap<>();
  private ZeebeClient client;
  private JobWorker worker;
  private TTStarterCfg ttCfg;
  private String processId;
  private int numTasks;
  private ScheduledExecutorService timeoutExecutor;

  private final io.prometheus.client.Counter totalWorkflowsCompleted =
      Counter.build()
          .namespace("ttstarter")
          .name("workflows_completed")
          .help("Number of workflows completed")
          .register();

  private final io.prometheus.client.Counter totalWorkflowsStarted =
      Counter.build()
          .namespace("ttstarter")
          .name("workflows_started")
          .help("Number of workflows started")
          .register();

  private final io.prometheus.client.Counter totalWorkflowsFailed =
      Counter.build()
          .namespace("ttstarter")
          .name("workflows_failed")
          .help("Number of workflows failed")
          .labelNames("reason")
          .register();
  private String variables;

  public TTStarter(final AppCfg appCfg) {
    this.appCfg = appCfg;
    requestFutures = new ArrayBlockingQueue<>(5_000);
  }

  @Override
  public void run() {
    ttCfg = appCfg.getTtStarter();
    processId = ttCfg.getProcessId();
    numTasks = ttCfg.getNumTasks();

    client = createZeebeClient();

    printTopology(client);

    executorService = Executors.newScheduledThreadPool(ttCfg.getThreads());
    responseCheckExecutor = Executors.newScheduledThreadPool(1);
    timeoutExecutor = Executors.newScheduledThreadPool(1);

    final ClassLoader classLoader = getClass().getClassLoader();
    variables = readVariables(appCfg.getTtStarter(), classLoader);

    final BpmnModelInstance workflow = createWorkflow();
    deployWorkflow(client, workflow);

    // start instances
    LOG.info("Creating 250 instances");

    startResponseWorker(client, ttCfg.getWorker());

    if (ttCfg.getInstanceTimeout().toMillis() > 0) {
      timeoutExecutor.scheduleAtFixedRate(
          this::timeoutRunningInstances, 500, 500, TimeUnit.MILLISECONDS);
    }

    for (int i = 0; i < 250; i++) {
      createInstance();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  worker.close();
                  client.close();
                  executorService.shutdown();
                  responseCheckExecutor.shutdown();
                }));
  }

  private String readVariables(final TTStarterCfg starterCfg, final ClassLoader classLoader) {
    try {
      final StringBuilder stringBuilder = new StringBuilder();
      try (final InputStream variablesStream =
          classLoader.getResourceAsStream(starterCfg.getPayloadPath())) {
        if (variablesStream == null) {
          throw new IllegalStateException(
              "Expected to access "
                  + starterCfg.getPayloadPath()
                  + ", but failed to open an input stream.");
        }

        try (final BufferedReader br = new BufferedReader(new InputStreamReader(variablesStream))) {
          String line;
          while ((line = br.readLine()) != null) {
            stringBuilder.append(line).append("\n");
          }
        }
      }

      return stringBuilder.toString();
    } catch (final IOException e) {
      throw new UncheckedExecutionException(e);
    }
  }

  private void timeoutRunningInstances() {
    final long currentTime = System.currentTimeMillis();
    final long timeout = ttCfg.getInstanceTimeout().toMillis();
    final Set<Long> toRemove = new HashSet<>();
    runningWorkflows.forEach(
        (w, startTime) -> {
          if (currentTime - startTime > timeout) {
            toRemove.add(w);
          }
        });
    toRemove.forEach(
        w -> {
          if (runningWorkflows.remove(w) != null) {
            LOG.info("Workflow instance {} timed out", w);
            totalWorkflowsFailed.labels("timeout").inc();
            cancelInstance(w);
            createInstance();
          }
        });
  }

  private void cancelInstance(final Long w) {
    client.newCancelInstanceCommand(w).send();
  }

  private void startResponseWorker(final ZeebeClient client, final WorkerCfg workerCfg) {
    final long completionDelay = workerCfg.getCompletionDelay().toMillis();
    worker =
        client
            .newWorker()
            .jobType(workerCfg.getJobType())
            .handler(
                (jobClient, job) -> {
                  try {
                    Thread.sleep(completionDelay);
                  } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted during completion delay, most likely shutting down", e);
                    return;
                  }
                  jobClient.newCompleteCommand(job.getKey()).variables(job.getVariables()).send();

                  final Long startTime = runningWorkflows.remove(job.getWorkflowInstanceKey());
                  if (startTime != null) {
                    LOG.info("Completed last job for workflow {}", job.getWorkflowInstanceKey());
                    totalWorkflowsCompleted.inc();
                    createInstance();
                  } else {
                    LOG.info(
                        "Last job for workflow instance {} completed, but workflow not found in running workflows",
                        job.getWorkflowInstanceKey());
                  }
                })
            .open();
  }

  private void checkResponse() {
    try {
      final WorkflowInstanceEvent workflowInstanceEvent = requestFutures.take().get();
      runningWorkflows.put(
          workflowInstanceEvent.getWorkflowInstanceKey(), System.currentTimeMillis());
    } catch (final InterruptedException e) {
      e.printStackTrace();
    } catch (final ExecutionException e) {
      totalWorkflowsFailed.labels("error").inc();
      LOG.info("Failed to create workflow instance, creating a new one", e);
      createInstance();
    }
  }

  private BpmnModelInstance createWorkflow() {

    switch (numTasks) {
      case 1:
        return createWorkflowWithOneTask();
      case 2:
        return createWorkflowWithTwoTasks();
      case 5:
        return createWorkflowWithFiveTasks();
      case 10:
        return createWorkflowWithTenTasks();
      default:
        LOG.info(
            "Configured {} tasks, only support 1,2,5,10 tasks. Creating workflow with 2 tasks",
            numTasks);
        return createWorkflowWithTwoTasks();
    }
  }

  private BpmnModelInstance createWorkflowWithOneTask() {
    LOG.info("Deploying workflow with one task and one response checker");
    final String jobType = appCfg.getWorker().getJobType();
    return Bpmn.createExecutableProcess(ttCfg.getProcessId())
        .startEvent()
        .serviceTask("task-1", b -> b.zeebeTaskType(jobType + 1))
        .serviceTask("responseChecker", b -> b.zeebeTaskType(ttCfg.getWorker().getJobType()))
        .endEvent()
        .done();
  }

  private BpmnModelInstance createWorkflowWithTwoTasks() {
    LOG.info("Deploying workflow with two sequential tasks and one response checker");
    final String jobType = appCfg.getWorker().getJobType();
    return Bpmn.createExecutableProcess(ttCfg.getProcessId())
        .startEvent()
        .serviceTask("task-1", b -> b.zeebeTaskType(jobType + 1))
        .serviceTask("task-2", b -> b.zeebeTaskType(jobType + 2))
        .serviceTask("responseChecker", b -> b.zeebeTaskType(ttCfg.getWorker().getJobType()))
        .endEvent()
        .done();
  }

  private BpmnModelInstance createWorkflowWithFiveTasks() {
    LOG.info("Deploying workflow with 5 sequential tasks and one response checker");
    final String jobType = appCfg.getWorker().getJobType();
    return Bpmn.createExecutableProcess(ttCfg.getProcessId())
        .startEvent()
        .serviceTask("task-1", b -> b.zeebeTaskType(jobType + 1))
        .serviceTask("task-2", b -> b.zeebeTaskType(jobType + 2))
        .serviceTask("task-3", b -> b.zeebeTaskType(jobType + 3))
        .serviceTask("task-4", b -> b.zeebeTaskType(jobType + 4))
        .serviceTask("task-5", b -> b.zeebeTaskType(jobType + 5))
        .serviceTask("responseChecker", b -> b.zeebeTaskType(ttCfg.getWorker().getJobType()))
        .endEvent()
        .done();
  }

  private BpmnModelInstance createWorkflowWithTenTasks() {
    LOG.info("Deploying workflow with 10 sequential tasks and one response checker");
    final String jobType = appCfg.getWorker().getJobType();
    return Bpmn.createExecutableProcess(ttCfg.getProcessId())
        .startEvent()
        .serviceTask("task-1", b -> b.zeebeTaskType(jobType + 1))
        .serviceTask("task-2", b -> b.zeebeTaskType(jobType + 2))
        .serviceTask("task-3", b -> b.zeebeTaskType(jobType + 3))
        .serviceTask("task-4", b -> b.zeebeTaskType(jobType + 4))
        .serviceTask("task-5", b -> b.zeebeTaskType(jobType + 5))
        .serviceTask("task-6", b -> b.zeebeTaskType(jobType + 6))
        .serviceTask("task-7", b -> b.zeebeTaskType(jobType + 7))
        .serviceTask("task-8", b -> b.zeebeTaskType(jobType + 8))
        .serviceTask("task-9", b -> b.zeebeTaskType(jobType + 9))
        .serviceTask("task-10", b -> b.zeebeTaskType(jobType + 10))
        .serviceTask("responseChecker", b -> b.zeebeTaskType(ttCfg.getWorker().getJobType()))
        .endEvent()
        .done();
  }

  private void createInstance() {
    executorService.submit(
        () -> {
          try {
            requestFutures.put(
                client
                    .newCreateInstanceCommand()
                    .bpmnProcessId(processId)
                    .latestVersion()
                    .variables(variables)
                    .send());
            responseCheckExecutor.submit(this::checkResponse);
            totalWorkflowsStarted.inc();
          } catch (final Exception e) {
            LOG.error("Error on creating new workflow instance", e);
          }
        });
  }

  private ZeebeClient createZeebeClient() {
    return ZeebeClient.newClientBuilder()
        .brokerContactPoint(appCfg.getBrokerUrl())
        .numJobWorkerExecutionThreads(ttCfg.getWorker().getThreads())
        .defaultJobWorkerName(ttCfg.getWorker().getWorkerName())
        .defaultJobTimeout(ttCfg.getWorker().getCompletionDelay().multipliedBy(6))
        .defaultJobWorkerMaxJobsActive(ttCfg.getWorker().getCapacity())
        .defaultJobPollInterval(ttCfg.getWorker().getPollingDelay())
        .withProperties(System.getProperties())
        .build();
  }

  private void deployWorkflow(final ZeebeClient client, final BpmnModelInstance workflow) {
    while (true) {
      try {
        client.newDeployCommand().addWorkflowModel(workflow, "tt.bpmn").send().join();
        break;
      } catch (final Exception e) {
        LOG.warn("Failed to deploy workflow, retrying", e);
        try {
          Thread.sleep(200);
        } catch (final InterruptedException ex) {
          // ignore
        }
      }
    }
  }

  public static void main(final String[] args) {
    createApp(TTStarter::new);
  }
}
