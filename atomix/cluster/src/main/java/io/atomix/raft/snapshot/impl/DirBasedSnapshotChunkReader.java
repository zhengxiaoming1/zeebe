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

import io.atomix.raft.snapshot.SnapshotChunk;
import io.atomix.raft.snapshot.SnapshotChunkReader;
import io.zeebe.protocol.Protocol;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Implements a chunk reader where each chunk is a single file in a root directory. Chunks are then
 * ordered lexicographically, and the files are assumed to be immutable, i.e. no more are added to
 * the directory once this is created.
 */
public final class DirBasedSnapshotChunkReader implements SnapshotChunkReader {
  public static final Charset ID_CHARSET = StandardCharsets.US_ASCII;
  private final Path directory;
  private final NavigableSet<CharSequence> chunks;
  private final CharSequenceView chunkIdView;

  private NavigableSet<CharSequence> chunksView;

  public DirBasedSnapshotChunkReader(
      final Path directory, final NavigableSet<CharSequence> chunks) {
    this.directory = directory;
    this.chunks = chunks;
    this.chunksView = this.chunks;
    this.chunkIdView = new CharSequenceView();
  }

  @Override
  public void seek(final ByteBuffer id) {
    if (id == null) {
      return;
    }

    final var path = decodeChunkId(id);
    chunksView = chunks.tailSet(path, true);
  }

  @Override
  public ByteBuffer nextId() {
    if (chunksView.isEmpty()) {
      return null;
    }

    return encodeChunkId(chunksView.first());
  }

  @Override
  public void close() {
    // nothing to do
  }

  @Override
  public boolean hasNext() {
    return !chunksView.isEmpty();
  }

  @Override
  public SnapshotChunk next() {
    final var id = chunksView.pollFirst();
    if (id == null) {
      throw new NoSuchElementException();
    }

    final var path = directory.resolve(id.toString());

    try {
      // todo(zell) make the buffer and chunk a field and reuse it
      final var bytes = Files.readAllBytes(path);
      final var readBuffer = new UnsafeBuffer(bytes);
      final var currentChunk = new SnapshotChunkImpl();
      currentChunk.wrap(readBuffer, 0, bytes.length);
      return currentChunk;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ByteBuffer encodeChunkId(final CharSequence path) {
    return ByteBuffer.wrap(path.toString().getBytes(ID_CHARSET)).order(Protocol.ENDIANNESS);
  }

  private CharSequence decodeChunkId(final ByteBuffer id) {
    return chunkIdView.wrap(id);
  }

  private static final class CharSequenceView {
    private final DirectBuffer wrapper = new UnsafeBuffer();
    private final AsciiSequenceView view = new AsciiSequenceView();

    private CharSequence wrap(final ByteBuffer buffer) {
      wrapper.wrap(buffer);
      return view.wrap(wrapper, 0, wrapper.capacity());
    }
  }
}
