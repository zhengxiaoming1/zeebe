package io.atomix.raft.snapshot;

/**
 * An volatile snapshot which can be persisted.
 */
public interface PersistableSnapshot {

  /**
   * Aborts the not yet persisted snapshot and removes all related data.
   */
  void abort();

  /**
   * Persists the snapshot with all his data and returns the representation of this snapshot.
   *
   * @return the persisted snapshot
   */
  PersistedSnapshot persist();
}
