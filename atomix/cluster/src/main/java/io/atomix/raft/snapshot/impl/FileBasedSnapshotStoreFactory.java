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

import io.atomix.raft.snapshot.PersistedSnapshotStore;
import io.atomix.raft.snapshot.PersistedSnapshotStoreFactory;
import io.atomix.raft.snapshot.ReceiverSnapshotStore;
import io.atomix.raft.snapshot.WritableSnapshotStore;
import java.nio.file.Path;
import org.agrona.IoUtil;

/**
 * Loads existing snapshots in memory, cleaning out old and/or invalid snapshots if present.
 *
 * <p>The current load strategy is to lookup all files directly under the {@code
 * SNAPSHOTS_DIRECTORY}, try to extract {@link FileBasedSnapshotMetadata} from them, and if not
 * possible skip them (and print out a warning).
 *
 * <p>The metadata extraction is done by parsing the directory name using '%d-%d-%d-%d', where in
 * order we expect: index, term, timestamp, and position.
 */
public final class FileBasedSnapshotStoreFactory implements PersistedSnapshotStoreFactory {
  public static final String SNAPSHOTS_DIRECTORY = "snapshots";
  public static final String PENDING_DIRECTORY = "pending";
  private FileBasedSnapshotStore fileBasedSnapshotStore;

  @Override
  public WritableSnapshotStore getWritableSnapshotStore(
      final Path root, final String partitionName) {
    return getPersistedSnapshotStore(root, partitionName);
  }

  @Override
  public ReceiverSnapshotStore getReceiverSnapshotStore(
      final Path root, final String partitionName) {
    return getPersistedSnapshotStore(root, partitionName);
  }

  @Override
  public PersistedSnapshotStore getReadonlySnapshotStore(
      final Path dataPath, final String partitionName) {
    return getPersistedSnapshotStore(dataPath, partitionName);
  }

  private FileBasedSnapshotStore getPersistedSnapshotStore(
      final Path dataPath, final String partitionName) {
    ensureSnapshotStoreExists(dataPath, partitionName);
    return fileBasedSnapshotStore;
  }

  private void ensureSnapshotStoreExists(final Path root, final String partitionName) {
    if (fileBasedSnapshotStore == null) {
      final var snapshotDirectory = root.resolve(SNAPSHOTS_DIRECTORY);
      final var pendingDirectory = root.resolve(PENDING_DIRECTORY);

      IoUtil.ensureDirectoryExists(snapshotDirectory.toFile(), "Snapshot directory");
      IoUtil.ensureDirectoryExists(pendingDirectory.toFile(), "Pending snapshot directory");

      fileBasedSnapshotStore =
          new FileBasedSnapshotStore(
              new SnapshotMetrics(partitionName), snapshotDirectory, pendingDirectory);
    }
  }
}
