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

/**
 * Represents a store, which allows to persist snapshots on a storage, which is implementation
 * dependent. It is possible to take a transient snapshot, which means you can start taking an
 * snapshot and can persist it later or abort it.
 *
 * <p>Only one {@link PersistedSnapshot} at a time is stored in the {@link WritableSnapshotStore}
 * and can be received via {@link WritableSnapshotStore#getLatestSnapshot()}.
 */
public interface WritableSnapshotStore extends PersistedSnapshotStore {

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
}
