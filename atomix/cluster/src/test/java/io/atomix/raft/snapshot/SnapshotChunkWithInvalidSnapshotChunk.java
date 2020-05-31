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
