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

import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.util.CloseableSilently;
import java.io.IOException;
import java.util.Optional;

public interface PersistedSnapshotStore extends CloseableSilently {

  /**
   * Returns true if a snapshot with the given ID exists already, false otherwise.
   *
   * @param id the snapshot ID to look for
   * @return true if there is a committed snapshot with this ID, false otherwise
   */
  boolean exists(String id);

  /**
   * Starts a new transient snapshot which can be persisted after the snapshot was taken.
   *
   * @param index the index to which the snapshot corresponds to
   * @param term the term to which the snapshots corresponds to
   * @param timestamp the time to which the snapshots corresponds to
   * @return the new transient snapshot
   */
  TransientSnapshot newTransientSnapshot(
      final long index, final long term, final WallClockTimestamp timestamp);

  /**
   * Starts a new received volatile snapshot which can be persisted later.
   *
   * @param snapshotId the snapshot id which is defined as {@code index-term-timestamp}
   * @return the new volatile received snapshot
   */
  ReceivedSnapshot newReceivedSnapshot(String snapshotId);

  Optional<PersistedSnapshot> getLatestSnapshot();

  void purgePendingSnapshots() throws IOException;

  void addSnapshotListener(PersistedSnapshotListener listener);

  void removeSnapshotListener(PersistedSnapshotListener listener);

  long getCurrentSnapshotIndex();

  /**
   * Deletes a {@link PersistedSnapshotStore} from disk.
   *
   * <p>The snapshot store will be deleted by simply reading {@code snapshot} file names from disk
   * and deleting snapshot files directly. Deleting the snapshot store does not involve reading any
   * snapshot files into memory.
   */
  void delete();
}
