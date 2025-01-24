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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.trinitylake.ObjectDefinitions;
import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.storage.FilePaths;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.local.LocalInputStream;
import io.trinitylake.util.FileUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;

public class TreeOperations {

  public static TreeNode clone(TreeNode node) {
    TreeNode clonedNode = new BasicTreeNode();
    node.allKeyValues()
        .forEach(
            entry -> {
              if (!TreeKeys.VERSION.equals(entry.getKey())) {
                clonedNode.set(entry.getKey(), entry.getValue());
              }
            });
    return clonedNode;
  }

  public static TreeNode readNodeFile(LakehouseStorage storage, String path) {
    TreeNode treeNode = new BasicTreeNode();

    BufferAllocator allocator = new RootAllocator();
    try (LocalInputStream stream = storage.startReadLocal(path)) {
      ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator);
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        for (int i = 0; i < root.getRowCount(); ++i) {
          String key = (String) root.getVector(0).getObject(i);
          String value = (String) root.getVector(1).getObject(i);
          treeNode.set(key, value);
        }
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }

    return treeNode;
  }

  public static void writeNodeFile(LakehouseStorage storage, String path, TreeNode node) {
    node.set(TreeKeys.CREATED_AT_MILLIS, Long.toString(System.currentTimeMillis()));
    OutputStream stream = storage.startWrite(path);
    BufferAllocator allocator = new RootAllocator();
    VarCharVector keyVector = new VarCharVector("key", allocator);
    VarCharVector valueVector = new VarCharVector("value", allocator);

    int index = 0;
    for (Map.Entry<String, String> entry : node.allKeyValues()) {
      keyVector.setSafe(index, entry.getKey().getBytes(StandardCharsets.UTF_8));
      valueVector.setSafe(index, entry.getValue().getBytes(StandardCharsets.UTF_8));
      index++;
    }

    keyVector.setValueCount(index);
    valueVector.setValueCount(index);

    List<Field> fields = Lists.newArrayList(keyVector.getField(), valueVector.getField());
    List<FieldVector> vectors = Lists.newArrayList(keyVector, valueVector);
    VectorSchemaRoot schema = new VectorSchemaRoot(fields, vectors);
    // TODO: set dictionary provider
    try (ArrowStreamWriter writer =
        new ArrowStreamWriter(schema, null, Channels.newChannel(stream))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } catch (IOException e) {
      throw new StorageWriteFailureException(e);
    }
  }

  public static boolean hasCreatedAtMillis(TreeNode node) {
    return node.contains(TreeKeys.CREATED_AT_MILLIS);
  }

  public static long findCreatedAtMillis(TreeNode node) {
    return Long.parseLong(node.get(TreeKeys.CREATED_AT_MILLIS));
  }

  public static boolean hasVersion(TreeNode node) {
    return node.contains(TreeKeys.VERSION);
  }

  public static long findVersion(TreeNode node) {
    return Long.parseLong(node.get(TreeKeys.VERSION));
  }

  public static boolean hasLakehouseDef(LakehouseStorage storage, TreeNode node) {
    return node.contains(TreeKeys.LAKEHOUSE_DEFINITION);
  }

  public static LakehouseDef findLakehouseDef(LakehouseStorage storage, TreeNode node) {
    return ObjectDefinitions.readLakehouseDef(storage, node.get(TreeKeys.LAKEHOUSE_DEFINITION));
  }

  public static TreeNode findPreviousRootNode(LakehouseStorage storage, TreeNode node) {
    return readNodeFile(storage, node.get(TreeKeys.PREVIOUS_ROOT_NODE));
  }

  public static boolean hasPreviousRootNode(TreeNode node) {
    return node.contains(TreeKeys.PREVIOUS_ROOT_NODE);
  }

  public static TreeNode findRollbackFromRootNode(LakehouseStorage storage, TreeNode node) {
    return readNodeFile(storage, node.get(TreeKeys.ROLLBACK_FROM_ROOT_NODE));
  }

  public static boolean hasRollbackFromRootNode(TreeNode node) {
    return node.contains(TreeKeys.ROLLBACK_FROM_ROOT_NODE);
  }

  public static TreeNode findLatestRoot(LakehouseStorage storage) {
    String latestVersionHintText =
        FileUtil.readToString(storage.startRead(FilePaths.LATEST_VERSION_HINT_FILE_PATH));
    long latestVersion = Long.parseLong(latestVersionHintText);
    String rootNodeFilePath = FilePaths.rootNodeFilePath(latestVersion);
    while (storage.exists(rootNodeFilePath)) {
      latestVersion++;
      rootNodeFilePath = FilePaths.rootNodeFilePath(latestVersion);
    }

    return readNodeFile(storage, rootNodeFilePath);
  }

  public static Optional<TreeNode> findRootForVersion(LakehouseStorage storage, long version) {
    TreeNode latest = findLatestRoot(storage);
    long latestVersion = findVersion(latest);
    Preconditions.checkArgument(
        version <= latestVersion,
        "Version %d must not be higher than latest version %d",
        version,
        latestVersion);

    TreeNode current = latest;
    while (hasPreviousRootNode(current)) {
      TreeNode previous = findPreviousRootNode(storage, current);
      if (version == findVersion(previous)) {
        return Optional.of(previous);
      }
      current = previous;
    }

    return Optional.empty();
  }

  public static TreeNode findRootBeforeTimestamp(LakehouseStorage storage, long timestampMillis) {
    TreeNode latest = findLatestRoot(storage);
    long latestCreatedAtMillis = findCreatedAtMillis(latest);
    Preconditions.checkArgument(
        timestampMillis <= latestCreatedAtMillis,
        "Timestamp %d must not be higher than latest created version %d",
        timestampMillis,
        latestCreatedAtMillis);

    TreeNode current = latest;
    while (hasPreviousRootNode(current)) {
      TreeNode previous = findPreviousRootNode(storage, current);
      if (timestampMillis > findCreatedAtMillis(previous)) {
        return previous;
      }
      current = previous;
    }

    return current;
  }

  public static Iterable<TreeNode> listRoots(LakehouseStorage storage) {
    return new TreeRootIterable(storage, findLatestRoot(storage));
  }

  private static class TreeRootIterable implements Iterable<TreeNode> {

    private final LakehouseStorage storage;
    private final TreeNode latest;

    public TreeRootIterable(LakehouseStorage storage, TreeNode latest) {
      this.storage = storage;
      this.latest = latest;
    }

    @Override
    public Iterator<TreeNode> iterator() {
      return new LakehouseVersionIterator(storage, latest);
    }
  }

  private static class LakehouseVersionIterator implements Iterator<TreeNode> {

    private final LakehouseStorage storage;
    private final TreeNode latest;
    private TreeNode current;

    public LakehouseVersionIterator(LakehouseStorage storage, TreeNode latest) {
      this.storage = storage;
      this.latest = latest;
      this.current = null;
    }

    @Override
    public boolean hasNext() {
      return current == null || hasPreviousRootNode(current);
    }

    @Override
    public TreeNode next() {
      if (current == null) {
        this.current = latest;
      } else {
        this.current = findPreviousRootNode(storage, current);
      }
      return current;
    }
  }
}
