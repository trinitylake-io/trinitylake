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

import io.trinitylake.storage.AtomicOutputStream;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.local.LocalInputStream;
import io.trinitylake.storage.local.LocalStorageOps;
import java.io.File;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTreeOperations {

  @Test
  public void testWriteReadNodeFile(@TempDir Path tempDir) {
    TreeNode treeNode = new BasicTreeNode();
    for (int i = 0; i < 10; i++) {
      treeNode.set("k" + i, "val" + i);
    }

    File file = tempDir.resolve("write.ipc").toFile();
    LocalStorageOps ops = new LocalStorageOps();
    AtomicOutputStream stream = ops.startWrite(new LiteralURI("file://" + file));

    TreeOperations.writeNodeFile(stream, treeNode);
    Assertions.assertThat(file.exists()).isTrue();

    LocalInputStream inputStream = ops.startReadLocal(new LiteralURI("file://" + file));
    TreeNode node = TreeOperations.readNodeFile(inputStream);
    Assertions.assertThat(
            treeNode.nodeKeyTable().stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)))
        .isEqualTo(
            node.nodeKeyTable().stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)));
  }
}
