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

package io.atomix.raft.snapshot.impl;

import io.atomix.raft.snapshot.SnapshotChunk;

public class InternalChunk implements SnapshotChunk, SnapshotChunk.Builder {

  private int totalCount;
  private String chunkName;

  @Override
  public String getSnapshotId() {
    return null;
  }

  @Override
  public int getTotalCount() {
    return totalCount;
  }

  @Override
  public String getChunkName() {
    return chunkName;
  }

  @Override
  public long getChecksum() {
    return 0;
  }

  @Override
  public byte[] getContent() {
    return new byte[0];
  }

  @Override
  public long getSnapshotChecksum() {
    return 0;
  }

  @Override
  public Builder withSnapshotId(final String snapshotId) {
    return null;
  }

  @Override
  public Builder withTotalCount(final int totalCount) {
    this.totalCount = totalCount;
    return this;
  }

  @Override
  public Builder withChunkName(final String chunkName) {
    this.chunkName = chunkName;
    return this;
  }

  @Override
  public Builder withChecksum(final long checksum) {
    return null;
  }

  @Override
  public Builder withContent(final byte[] content) {
    return null;
  }

  @Override
  public Builder withSnapshotChecksum(final long snapshotChecksum) {
    return null;
  }

  @Override
  public SnapshotChunk build() {
    return null;
  }
}
