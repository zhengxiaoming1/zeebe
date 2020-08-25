/*
 * Copyright © 2020  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.zeebe.gateway;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.protocol.GatewayGrpc.GatewayImplBase;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.CancelWorkflowInstanceRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CancelWorkflowInstanceResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.CompleteJobRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CompleteJobResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceWithResultRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceWithResultResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.DeployWorkflowResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.FailJobRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.FailJobResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.PublishMessageRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.PublishMessageResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.ResolveIncidentRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ResolveIncidentResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.SetVariablesRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.SetVariablesResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.ThrowErrorRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ThrowErrorResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.TopologyRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.TopologyResponse;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobRetriesRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobRetriesResponse;
import io.zeebe.util.exception.UncheckedExecutionException;

public class EndpointWrapper extends GatewayImplBase {
  private static final String ERROR_MSG_NOT_MAPPED =
      "Expected that the returned error response %s is mapped correctly as StatusRuntimeException, but was not.";

  private final EndpointManager endpointManager;

  public EndpointWrapper(final EndpointManager endpointManager) {
    this.endpointManager = endpointManager;
  }

  @Override
  public void activateJobs(
      final ActivateJobsRequest request,
      final StreamObserver<ActivateJobsResponse> responseObserver) {
    endpointManager.activateJobs(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void cancelWorkflowInstance(
      final CancelWorkflowInstanceRequest request,
      final StreamObserver<CancelWorkflowInstanceResponse> responseObserver) {
    endpointManager.cancelWorkflowInstance(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void completeJob(
      final CompleteJobRequest request,
      final StreamObserver<CompleteJobResponse> responseObserver) {
    endpointManager.completeJob(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void createWorkflowInstance(
      final CreateWorkflowInstanceRequest request,
      final StreamObserver<CreateWorkflowInstanceResponse> responseObserver) {
    endpointManager.createWorkflowInstance(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void createWorkflowInstanceWithResult(
      final CreateWorkflowInstanceWithResultRequest request,
      final StreamObserver<CreateWorkflowInstanceWithResultResponse> responseObserver) {
    endpointManager.createWorkflowInstanceWithResult(
        request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void deployWorkflow(
      final DeployWorkflowRequest request,
      final StreamObserver<DeployWorkflowResponse> responseObserver) {
    endpointManager.deployWorkflow(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void failJob(
      final FailJobRequest request, final StreamObserver<FailJobResponse> responseObserver) {
    endpointManager.failJob(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void throwError(
      final ThrowErrorRequest request, final StreamObserver<ThrowErrorResponse> responseObserver) {
    endpointManager.throwError(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void publishMessage(
      final PublishMessageRequest request,
      final StreamObserver<PublishMessageResponse> responseObserver) {
    endpointManager.publishMessage(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void resolveIncident(
      final ResolveIncidentRequest request,
      final StreamObserver<ResolveIncidentResponse> responseObserver) {
    endpointManager.resolveIncident(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void setVariables(
      final SetVariablesRequest request,
      final StreamObserver<SetVariablesResponse> responseObserver) {
    endpointManager.setVariables(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void topology(
      final TopologyRequest request, final StreamObserver<TopologyResponse> responseObserver) {
    endpointManager.topology(request, WrappedResponseObserver.of(responseObserver));
  }

  @Override
  public void updateJobRetries(
      final UpdateJobRetriesRequest request,
      final StreamObserver<UpdateJobRetriesResponse> responseObserver) {
    endpointManager.updateJobRetries(request, WrappedResponseObserver.of(responseObserver));
  }

  private static final class WrappedResponseObserver<T> implements StreamObserver<T> {

    private final StreamObserver<T> delegate;

    private WrappedResponseObserver(final StreamObserver<T> delegate) {
      this.delegate = delegate;

      if (delegate instanceof ServerCallStreamObserver) {
        final ServerCallStreamObserver<T> serverObserver = (ServerCallStreamObserver<T>) delegate;
        serverObserver.setOnCancelHandler(
            () -> Loggers.GATEWAY_LOGGER.trace("gRPC {} request cancelled", delegate.getClass()));
      }
    }

    public static <T> WrappedResponseObserver<T> of(final StreamObserver<T> toWrappedObserver) {
      return new WrappedResponseObserver<>(toWrappedObserver);
    }

    @Override
    public void onNext(final T value) {
      delegate.onNext(value);
    }

    @Override
    public void onError(final Throwable t) {
      if (t instanceof StatusRuntimeException) {
        delegate.onError(t);
      }

      // we haven't mapped the exception correctly
      delegate.onError(
          new UncheckedExecutionException(String.format(ERROR_MSG_NOT_MAPPED, t.getMessage()), t));
      throw new UncheckedExecutionException(String.format(ERROR_MSG_NOT_MAPPED, t.getMessage()), t);
    }

    @Override
    public void onCompleted() {
      delegate.onCompleted();
    }
  }
}
