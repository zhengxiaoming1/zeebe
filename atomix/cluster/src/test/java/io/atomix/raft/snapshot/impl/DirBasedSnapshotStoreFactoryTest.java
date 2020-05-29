/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.atomix.raft.snapshot.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class DirBasedSnapshotStoreFactoryTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void shouldCreateDirectoriesIfNotExist() {
    // given
    final var root = temporaryFolder.getRoot().toPath();
    final var factory = new DirBasedSnapshotStoreFactory();

    // when
    final var store = factory.createSnapshotStore(root, "ignored");

    // then
    assertThat(root.resolve(DirBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY))
        .exists()
        .isDirectory();
    assertThat(root.resolve(DirBasedSnapshotStoreFactory.PENDING_DIRECTORY)).exists().isDirectory();
    assertThat(store.getLatestSnapshot()).isEmpty();
  }
}
