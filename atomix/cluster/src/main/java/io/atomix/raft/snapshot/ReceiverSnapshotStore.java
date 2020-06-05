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

package io.atomix.raft.snapshot;

/**
 * Represents a store, which allows to persist snapshots on a storage, which is implementation
 * dependent. It is possible to take a transient snapshot, which means you can start taking an
 * snapshot and can persist it later or abort it. Furthermore it is possible to persist/receive
 * {@link SnapshotChunk}'s from an already {@link PersistedSnapshot} and persist them in this
 * current store.
 *
 * <p>Only one {@link PersistedSnapshot} at a time is stored in the {@link ReceiverSnapshotStore}
 * and can be received via {@link ReceiverSnapshotStore#getLatestSnapshot()}.
 */
public interface ReceiverSnapshotStore extends PersistedSnapshotStore {

  /**
   * Starts a new received volatile snapshot which can be persisted later.
   *
   * @param snapshotId the snapshot id which is defined as {@code index-term-timestamp}
   * @return the new volatile received snapshot
   */
  ReceivedSnapshot newReceivedSnapshot(String snapshotId);
}
