package io.atomix.raft.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.raft.snapshot.impl.DirBasedSnapshotStoreFactory;
import io.atomix.utils.time.WallClockTimestamp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SnapshotStoreTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private SnapshotStore snapshotStore;

  @Before
  public void before() {
    final DirBasedSnapshotStoreFactory factory = new DirBasedSnapshotStoreFactory();

    final var partitionName = "1";
    final var root = temporaryFolder.getRoot();

    snapshotStore = factory.createSnapshotStore(root.toPath(), partitionName);
  }

  @Test
  public void shouldReturnZeroWhenNoSnapshotWasTaken() {
    // given

    // when
    final var currentSnapshotIndex = snapshotStore.getCurrentSnapshotIndex();

    // then
    assertThat(currentSnapshotIndex).isEqualTo(0);
  }

  @Test
  public void shouldReturnEmptyWhenNoSnapshotWasTaken() {
    // given

    // when
    final var optionalLatestSnapshot = snapshotStore.getLatestSnapshot();

    // then
    assertThat(optionalLatestSnapshot).isEmpty();
  }

  @Test
  public void shouldReturnFalseOnNonExistingSnapshot() {
    // given

    // when
    final var exists = snapshotStore.exists("notexisting");

    // then
    assertThat(exists).isFalse();
  }

  @Test
  public void shouldCreateSubFoldersOnCreatingDirBasedStore() {
    // given

    // when + then
    assertThat(temporaryFolder.getRoot().toPath().resolve(DirBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY)).exists();
    assertThat(temporaryFolder.getRoot().toPath().resolve(DirBasedSnapshotStoreFactory.PENDING_DIRECTORY)).exists();
  }


  @Test
  public void shouldTakeTransientSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var time = WallClockTimestamp.from(123);

    // when
    final var transientSnapshot = snapshotStore.takeTransientSnapshot(index, term, time);

    // then
    assertThat(transientSnapshot.index()).isEqualTo(index);
  }

}
