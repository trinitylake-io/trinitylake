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

import io.trinitylake.FileLocations;
import io.trinitylake.storage.BasicLakehouseStorage;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.local.LocalStorageOps;
import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTreeOperations {

  @Test
  public void testWriteReadRootNodeFile(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    for (int i = 0; i < 10; i++) {
      treeRoot.set("k" + i, "val" + i);
    }
    treeRoot.setPreviousRootNodeFilePath("some/path/to/previous/root");
    treeRoot.setRollbackFromRootNodeFilePath("some/path/to/rollback/from/root");
    treeRoot.setLakehouseDefFilePath("some/path/to/lakehouse/def");

    File file = tempDir.resolve("testWriteReadRootNodeFile.ipc").toFile();

    TreeOperations.writeRootNodeFile(storage, "testWriteReadRootNodeFile.ipc", treeRoot);
    Assertions.assertThat(file.exists()).isTrue();

    TreeRoot root = TreeOperations.readRootNodeFile(storage, "testWriteReadRootNodeFile.ipc");
    Assertions.assertThat(
            treeRoot.nodeKeyTable().stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)))
        .isEqualTo(
            root.nodeKeyTable().stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)));
  }

  @Test
  public void testFindLatestVersion(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRootV0 = new BasicTreeRoot();
    treeRootV0.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    String v0Path = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, v0Path, treeRootV0);

    TreeRoot treeRootV1 = new BasicTreeRoot();
    treeRootV1.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV1.setPreviousRootNodeFilePath(v0Path);
    String v1Path = FileLocations.rootNodeFilePath(1);
    TreeOperations.writeRootNodeFile(storage, v1Path, treeRootV1);

    TreeRoot treeRootV2 = new BasicTreeRoot();
    treeRootV2.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV2.setPreviousRootNodeFilePath(v1Path);
    String v2Path = FileLocations.rootNodeFilePath(2);
    TreeOperations.writeRootNodeFile(storage, v2Path, treeRootV2);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    Assertions.assertThat(root.path().get()).isEqualTo(v2Path);
  }

  @Test
  public void testTreeRootIterable(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    LakehouseStorage storage = new BasicLakehouseStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRootV0 = new BasicTreeRoot();
    treeRootV0.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    String v0Path = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, v0Path, treeRootV0);

    TreeRoot treeRootV1 = new BasicTreeRoot();
    treeRootV1.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV1.setPreviousRootNodeFilePath(v0Path);
    String v1Path = FileLocations.rootNodeFilePath(1);
    TreeOperations.writeRootNodeFile(storage, v1Path, treeRootV1);

    TreeRoot treeRootV2 = new BasicTreeRoot();
    treeRootV2.setLakehouseDefFilePath("some/path/to/lakehouse/def");
    treeRootV2.setPreviousRootNodeFilePath(v1Path);
    String v2Path = FileLocations.rootNodeFilePath(2);
    TreeOperations.writeRootNodeFile(storage, v2Path, treeRootV2);

    Iterator<TreeRoot> roots = TreeOperations.listRoots(storage).iterator();

    Assertions.assertThat(roots.hasNext()).isTrue();
    TreeRoot root = roots.next();
    Assertions.assertThat(root.path().get()).isEqualTo(v2Path);
    Assertions.assertThat(root.previousRootNodeFilePath().get()).isEqualTo(v1Path);

    Assertions.assertThat(roots.hasNext()).isTrue();
    root = roots.next();
    Assertions.assertThat(root.path().get()).isEqualTo(v1Path);
    Assertions.assertThat(root.previousRootNodeFilePath().get()).isEqualTo(v0Path);

    Assertions.assertThat(roots.hasNext()).isTrue();
    root = roots.next();
    Assertions.assertThat(root.path().get()).isEqualTo(v0Path);
    Assertions.assertThat(root.previousRootNodeFilePath().isPresent()).isFalse();

    Assertions.assertThat(roots.hasNext()).isFalse();
  }
}
