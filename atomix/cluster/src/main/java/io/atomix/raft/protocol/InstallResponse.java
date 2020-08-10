/*
 * Copyright 2015-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.raft.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import io.atomix.raft.RaftError;
import java.nio.ByteBuffer;

/**
 * Snapshot installation response.
 *
 * <p>Install responses are sent once a snapshot installation request has been received and
 * processed. Install responses provide no additional metadata aside from indicating whether or not
 * the request was successful.
 */
public class InstallResponse extends AbstractRaftResponse {

  // TODO: This is NOT backward compatible !!!!
  private final ByteBuffer nextChunkId;

  private final boolean succeeded;

  public InstallResponse(
      final Status status,
      final RaftError error,
      final ByteBuffer nextChunkId,
      final boolean succeeded) {
    super(status, error);
    this.nextChunkId = nextChunkId;
    this.succeeded = succeeded;
  }

  /**
   * Returns a new install response builder.
   *
   * @return A new install response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public ByteBuffer getNextChunkId() {
    return nextChunkId;
  }

  public boolean isSucceeded() {
    return succeeded;
  }

  /** Install response builder. */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, InstallResponse> {

    private ByteBuffer nextChunkId;
    private boolean succeeded;

    public Builder withSucceeded(final boolean succeeded) {
      this.succeeded = succeeded;
      return this;
    }

    public Builder withNextChunkId(final ByteBuffer nextChunkId) {
      this.nextChunkId = nextChunkId;
      return this;
    }

    @Override
    public InstallResponse build() {
      validate();
      return new InstallResponse(status, error, nextChunkId, succeeded);
    }

    @Override
    protected void validate() {
      super.validate();
      if (status == Status.OK) {
        if (!succeeded) {
          checkNotNull(nextChunkId);
        }
      }
    }
  }
}
