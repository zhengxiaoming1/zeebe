/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.atomix.raft.snapshot;

public class SnapshotChunkWithInvalidSnapshotChunk implements SnapshotChunk {

  private final SnapshotChunk snapshotChunk;

  public SnapshotChunkWithInvalidSnapshotChunk(final SnapshotChunk snapshotChunk) {
    this.snapshotChunk = snapshotChunk;
  }

  @Override
  public String getSnapshotId() {
    return snapshotChunk.getSnapshotId();
  }

  @Override
  public int getTotalCount() {
    return snapshotChunk.getTotalCount();
  }

  @Override
  public String getChunkName() {
    return snapshotChunk.getChunkName();
  }

  @Override
  public long getChecksum() {
    return snapshotChunk.getChecksum();
  }

  @Override
  public byte[] getContent() {
    return snapshotChunk.getContent();
  }

  @Override
  public long getSnapshotChecksum() {
    return 0xCAFE;
  }
}
