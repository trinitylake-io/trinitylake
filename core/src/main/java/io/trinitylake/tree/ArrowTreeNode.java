/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trinitylake.tree;

import com.google.common.collect.ImmutableMap;
import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.storage.SeekableFileInputStream;
import io.trinitylake.storage.Storage;
import io.trinitylake.util.Pair;
import java.io.IOException;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowTreeNode implements TreeNode {

  private static final Logger LOG = LoggerFactory.getLogger(ArrowTreeNode.class);

  // TODO: this is used as a quick implementation.
  //  we should avoid deserialization and directly use vector to access values
  private final Map<String, Pair<String, String>> values;
  private final long createdAtMillis;

  public ArrowTreeNode(Storage storage, String path) {

    ImmutableMap.Builder<String, Pair<String, String>> valuesBuilder = ImmutableMap.builder();

    BufferAllocator allocator = new RootAllocator();
    try (SeekableFileInputStream stream = storage.startReadLocal(path)) {
      ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator);
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        for (int i = 0; i < root.getRowCount(); ++i) {
          String key = (String) root.getVector(0).getObject(i);
          String value = (String) root.getVector(1).getObject(i);
          String nodePointer = (String) root.getVector(2).getObject(i);
          valuesBuilder.put(key, Pair.of(value, nodePointer));
        }
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }

    this.values = valuesBuilder.build();
    this.createdAtMillis = Long.parseLong(values.get(KeyNames.CREATED_AT_MILLIS).first());
  }

  @Override
  public long createdAtMillis() {
    return createdAtMillis;
  }

  @Override
  public Pair<String, String> findValue(String key) {
    return values.get(key);
  }
}
