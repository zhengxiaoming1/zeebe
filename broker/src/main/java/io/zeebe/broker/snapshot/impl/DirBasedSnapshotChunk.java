/// *
// * Copyright © 2020  camunda services GmbH (info@camunda.com)
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *        http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// */
// package io.zeebe.broker.snapshot.impl;
//
// import io.atomix.raft.snapshot.SnapshotChunk;
// import java.nio.ByteBuffer;
// import java.util.Objects;
//
// public final class DirBasedSnapshotChunk implements SnapshotChunk {
//  private final ByteBuffer id;
//  private final ByteBuffer data;
//
//  public DirBasedSnapshotChunk(final ByteBuffer id, final ByteBuffer data) {
//    this.id = id;
//    this.data = data;
//  }
//
//  @Override
//  public ByteBuffer id() {
//    return id;
//  }
//
//  @Override
//  public ByteBuffer data() {
//    return data;
//  }
//
//  @Override
//  public int hashCode() {
//    return Objects.hash(id, data);
//  }
//
//  @Override
//  public boolean equals(final Object o) {
//    if (this == o) {
//      return true;
//    }
//
//    if (o == null || getClass() != o.getClass()) {
//      return false;
//    }
//
//    final DirBasedSnapshotChunk that = (DirBasedSnapshotChunk) o;
//    return id.equals(that.id) && data.equals(that.data);
//  }
//
//  @Override
//  public String toString() {
//    return "DbSnapshotChunk{" + "id=" + id + ", data=" + data + '}';
//  }
//
//  @Override
//  public String getSnapshotId() {
//    return null;
//  }
//
//  @Override
//  public int getTotalCount() {
//    return 0;
//  }
//
//  @Override
//  public String getChunkName() {
//    return null;
//  }
//
//  @Override
//  public long getChecksum() {
//    return 0;
//  }
//
//  @Override
//  public byte[] getContent() {
//    return new byte[0];
//  }
//
//  @Override
//  public long getSnapshotChecksum() {
//    return 0;
//  }
// }
