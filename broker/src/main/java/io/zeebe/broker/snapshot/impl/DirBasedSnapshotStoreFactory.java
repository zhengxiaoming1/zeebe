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
package io.zeebe.broker.snapshot.impl;

import io.atomix.raft.impl.zeebe.snapshot.DbSnapshotMetadata;
import io.atomix.raft.snapshot.SnapshotId;
import io.atomix.raft.snapshot.SnapshotStore;
import io.atomix.raft.snapshot.SnapshotStoreFactory;
import io.zeebe.util.ZbLogger;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.agrona.IoUtil;
import org.slf4j.Logger;

/**
 * Loads existing snapshots in memory, cleaning out old and/or invalid snapshots if present.
 *
 * <p>The current load strategy is to lookup all files directly under the {@code
 * SNAPSHOTS_DIRECTORY}, try to extract {@link DbSnapshotMetadata} from them, and if not possible
 * skip them (and print out a warning).
 *
 * <p>The metadata extraction is done by parsing the directory name using '%d-%d-%d-%d', where in
 * order we expect: index, term, timestamp, and position.
 */
public final class DirBasedSnapshotStoreFactory implements SnapshotStoreFactory {
  public static final String SNAPSHOTS_DIRECTORY = "snapshots";
  public static final String PENDING_DIRECTORY = "pending";
  private static final Logger LOGGER = new ZbLogger(DirBasedSnapshotStoreFactory.class);

  @Override
  public SnapshotStore createSnapshotStore(final Path root, final String partitionName) {
    final var snapshots = new ConcurrentSkipListMap<SnapshotId, DirBasedSnapshot>();
    final var snapshotDirectory = root.resolve(SNAPSHOTS_DIRECTORY);
    final var pendingDirectory = root.resolve(PENDING_DIRECTORY);

    IoUtil.ensureDirectoryExists(snapshotDirectory.toFile(), "Snapshot directory");
    IoUtil.ensureDirectoryExists(pendingDirectory.toFile(), "Pending snapshot directory");

    loadSnapshots(snapshotDirectory, snapshots);

    return new DirBasedSnapshotStore(snapshotDirectory, pendingDirectory, snapshots);
  }

  private void loadSnapshots(
      final Path snapshotDirectory, final NavigableMap<SnapshotId, DirBasedSnapshot> snapshots) {
    try (final var stream = Files.newDirectoryStream(snapshotDirectory)) {
      for (final var path : stream) {
        collectSnapshot(snapshots, path);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void collectSnapshot(
      final NavigableMap<SnapshotId, DirBasedSnapshot> snapshots, final Path path) {
    final var optionalMeta = DirBasedSnapshotMetadata.ofPath(path);
    if (optionalMeta.isPresent()) {
      final var metadata = optionalMeta.get();
      snapshots.put(metadata, new DirBasedSnapshot(path, metadata));
    } else {
      LOGGER.warn("Expected snapshot file format to be %d-%d-%d-%d, but was {}", path);
    }
  }
}
