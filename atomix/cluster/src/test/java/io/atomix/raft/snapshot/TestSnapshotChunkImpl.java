package io.atomix.raft.snapshot;

class TestSnapshotChunkImpl implements SnapshotChunk {

  final int totalCount;
  final String chunkName;
  private final byte[] content;
  private final String snapshotId;

  TestSnapshotChunkImpl(
      final String snapshotId,
      final String chunkName,
      final byte[] content,
      final int totalCount) {
    this.content = content;
    this.snapshotId = snapshotId;
    this.totalCount = totalCount;
    this.chunkName = chunkName;
  }

  @Override
  public String getSnapshotId() {
    return snapshotId;
  }

  @Override
  public int getTotalCount() {
    return totalCount;
  }

  @Override
  public String getChunkName() {
    return chunkName;
  }

  @Override
  public long getChecksum() {
    return 0;
  }

  @Override
  public byte[] getContent() {
    return content;
  }

  @Override
  public long getSnapshotChecksum() {
    return 0;
  }
}
