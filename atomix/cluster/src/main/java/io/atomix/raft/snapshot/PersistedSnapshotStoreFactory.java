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

import java.nio.file.Path;

/** Can return snapshot stores to persist a new snapshot or receive existing ones. */
public interface PersistedSnapshotStoreFactory {

  /**
   * Creates a snapshot store, which can be used to write/persist snapshot to it, operating in the
   * given {@code directory}.
   *
   * @param directory the root directory where snapshots should be stored
   * @param partitionName the partition name for this store
   * @return a new {@link PersistedSnapshotStore}
   */
  WritableSnapshotStore getWritableSnapshotStore(Path directory, String partitionName);

  /**
   * Returns a snapshot store, which is used to receive snapshots, it operates in the given {@code
   * directory}.
   *
   * @param directory the root directory where snapshots should be stored
   * @param partitionName the partition name for this store
   * @return a new {@link PersistedSnapshotStore}
   */
  ReceiverSnapshotStore getReceiverSnapshotStore(Path directory, String partitionName);

  PersistedSnapshotStore getReadonlySnapshotStore(Path dataPath, String partitionName);
}
