package io.atomix.raft.snapshot;

public class InvalidSnapshotChunk implements SnapshotChunk {

  private final SnapshotChunk snapshotChunk;

  public InvalidSnapshotChunk(final SnapshotChunk snapshotChunk) {
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
    return 0xCAFE;
  }

  @Override
  public byte[] getContent() {
    return snapshotChunk.getContent();
  }

  @Override
  public long getSnapshotChecksum() {
    return snapshotChunk.getSnapshotChecksum();
  }
}
