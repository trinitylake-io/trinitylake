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

import com.google.common.collect.Lists;
import io.trinitylake.ObjectDefinitions;
import io.trinitylake.ObjectKeys;
import io.trinitylake.ObjectLocations;
import io.trinitylake.exception.StorageAtomicSealFailureException;
import io.trinitylake.exception.StorageReadFailureException;
import io.trinitylake.exception.StorageWriteFailureException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.local.LocalInputStream;
import io.trinitylake.util.FileUtil;
import io.trinitylake.util.ValidationUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;

public class TreeOperations {

  private static final int NODE_FILE_KEY_COLUMN_INDEX = 0;
  private static final String NODE_FILE_KEY_COLUMN_NAME = "key";

  private static final int NODE_FILE_VALUE_COLUMN_INDEX = 1;
  private static final String NODE_FILE_VALUE_COLUMN_NAME = "value";

  /**
   * Clone a tree node, excluding persistence specific information like path and creation time.
   *
   * @param node node to be cloned
   * @return cloned node
   */
  public static TreeNode clone(TreeNode node) {
    TreeNode clonedNode = new BasicTreeNode();
    node.nodeKeyTable().forEach(entry -> clonedNode.set(entry.key(), entry.value()));
    return clonedNode;
  }

  public static TreeNode readNodeFile(LocalInputStream stream) {
    TreeNode treeNode = new BasicTreeNode();

    BufferAllocator allocator = new RootAllocator();
    ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator);
    try {
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        int numKeys = 0;
        for (int i = 0; i < root.getRowCount(); ++i) {
          // TODO: leverage Arrow Text to be more efficient and avoid byte array copy
          String key = root.getVector(NODE_FILE_KEY_COLUMN_INDEX).getObject(i).toString();
          String value = root.getVector(NODE_FILE_VALUE_COLUMN_INDEX).getObject(i).toString();

          if (ObjectKeys.CREATED_AT_MILLIS.equals(key)) {
            treeNode.setCreatedAtMillis(Long.parseLong(value));
          } else if (ObjectKeys.NUMBER_OF_KEYS.equals(key)) {
            numKeys = Integer.parseInt(value);
          } else {
            treeNode.set(key, value);
          }
        }

        ValidationUtil.checkState(
            numKeys == treeNode.numKeys(),
            "Recorded number of keys do not match the actual node key table size, the node file might be corrupted");
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }

    return treeNode;
  }

  public static void writeNodeFile(AtomicOutputStream stream, TreeNode node) {
    BufferAllocator allocator = new RootAllocator();
    VarCharVector keyVector = new VarCharVector(NODE_FILE_KEY_COLUMN_NAME, allocator);
    VarCharVector valueVector = new VarCharVector(NODE_FILE_VALUE_COLUMN_NAME, allocator);

    int index = 0;
    long createdAtMillis = System.currentTimeMillis();
    keyVector.setSafe(index, ObjectKeys.CREATED_AT_MILLIS_BYTES);
    valueVector.setSafe(index, Long.toString(createdAtMillis).getBytes(StandardCharsets.UTF_8));

    index++;
    keyVector.setSafe(index, ObjectKeys.NUMBER_OF_KEYS_BYTES);
    valueVector.setSafe(index, Integer.toString(node.numKeys()).getBytes(StandardCharsets.UTF_8));

    for (NodeKeyTableRow row : node.nodeKeyTable()) {
      index++;
      keyVector.setSafe(index, row.key().getBytes(StandardCharsets.UTF_8));
      valueVector.setSafe(index, row.value().getBytes(StandardCharsets.UTF_8));
    }

    index++;
    keyVector.setValueCount(index);
    valueVector.setValueCount(index);

    List<Field> fields = Lists.newArrayList(keyVector.getField(), valueVector.getField());
    List<FieldVector> vectors = Lists.newArrayList(keyVector, valueVector);
    VectorSchemaRoot schema = new VectorSchemaRoot(fields, vectors);
    try (ArrowFileWriter writer = new ArrowFileWriter(schema, null, stream.channel())) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } catch (IOException e) {
      throw new StorageWriteFailureException(e);
    }

    try {
      stream.close();
    } catch (IOException e) {
      throw new StorageAtomicSealFailureException(e);
    }
  }

  public static boolean hasCreatedAtMillis(TreeNode node) {
    return node.contains(ObjectKeys.CREATED_AT_MILLIS);
  }

  public static long findCreatedAtMillis(TreeNode node) {
    return Long.parseLong(node.get(ObjectKeys.CREATED_AT_MILLIS));
  }

  public static boolean hasVersion(TreeNode node) {
    return node.path().isPresent() && ObjectLocations.isRootNodeFilePath(node.path().get());
  }

  public static long findVersionFromRootNode(TreeNode node) {
    ValidationUtil.checkArgument(
        node.path().isPresent(), "Cannot derive version from a node that is not persisted");
    ValidationUtil.checkArgument(
        ObjectLocations.isRootNodeFilePath(node.path().get()),
        "Cannot derive version from a non-root node");
    return ObjectLocations.versionFromNodeFilePath(node.path().get());
  }

  public static boolean hasLakehouseDef(LakehouseStorage storage, TreeNode node) {
    return node.contains(ObjectKeys.LAKEHOUSE_DEFINITION);
  }

  public static LakehouseDef findLakehouseDef(LakehouseStorage storage, TreeNode node) {
    return ObjectDefinitions.readLakehouseDef(storage, node.get(ObjectKeys.LAKEHOUSE_DEFINITION));
  }

  public static TreeNode findPreviousRootNode(LakehouseStorage storage, TreeNode node) {
    LocalInputStream stream = storage.startReadLocal(node.get(ObjectKeys.PREVIOUS_ROOT_NODE));
    return TreeOperations.readNodeFile(stream);
  }

  public static boolean hasPreviousRootNode(TreeNode node) {
    return node.contains(ObjectKeys.PREVIOUS_ROOT_NODE);
  }

  public static TreeNode findRollbackFromRootNode(LakehouseStorage storage, TreeNode node) {
    LocalInputStream stream = storage.startReadLocal(node.get(ObjectKeys.ROLLBACK_FROM_ROOT_NODE));
    return TreeOperations.readNodeFile(stream);
  }

  public static boolean hasRollbackFromRootNode(TreeNode node) {
    return node.contains(ObjectKeys.ROLLBACK_FROM_ROOT_NODE);
  }

  public static TreeNode findLatestRoot(LakehouseStorage storage) {
    String latestVersionHintText =
        FileUtil.readToString(storage.startRead(ObjectLocations.LATEST_VERSION_HINT_FILE_PATH));
    long latestVersion = Long.parseLong(latestVersionHintText);
    String rootNodeFilePath = ObjectLocations.rootNodeFilePath(latestVersion);
    while (storage.exists(rootNodeFilePath)) {
      latestVersion++;
      rootNodeFilePath = ObjectLocations.rootNodeFilePath(latestVersion);
    }

    LocalInputStream stream = storage.startReadLocal(rootNodeFilePath);
    return TreeOperations.readNodeFile(stream);
  }

  public static Optional<TreeNode> findRootForVersion(LakehouseStorage storage, long version) {
    TreeNode latest = findLatestRoot(storage);
    long latestVersion = findVersionFromRootNode(latest);
    ValidationUtil.checkArgument(
        version <= latestVersion,
        "Version %d must not be higher than latest version %d",
        version,
        latestVersion);

    TreeNode current = latest;
    while (hasPreviousRootNode(current)) {
      TreeNode previous = findPreviousRootNode(storage, current);
      if (version == findVersionFromRootNode(previous)) {
        return Optional.of(previous);
      }
      current = previous;
    }

    return Optional.empty();
  }

  public static TreeNode findRootBeforeTimestamp(LakehouseStorage storage, long timestampMillis) {
    TreeNode latest = findLatestRoot(storage);
    long latestCreatedAtMillis = findCreatedAtMillis(latest);
    ValidationUtil.checkArgument(
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
