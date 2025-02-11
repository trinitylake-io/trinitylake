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
package io.trinitylake;

import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.storage.BasicLakehouseStorage;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.local.LocalStorageOps;
import io.trinitylake.storage.local.LocalStorageOpsProperties;
import io.trinitylake.tree.TreeOperations;
import io.trinitylake.tree.TreeRoot;
import java.io.File;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTrinityLake {

  @TempDir private File tempDir;

  private LakehouseStorage storage;

  @BeforeEach
  public void beforeEach() {
    CommonStorageOpsProperties props =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir + "/tmp-write",
                CommonStorageOpsProperties.PREPARE_READ_STAGING_DIRECTORY, tempDir + "/tmp-read"));

    this.storage =
        new BasicLakehouseStorage(
            new LiteralURI("file://" + tempDir),
            new LocalStorageOps(props, LocalStorageOpsProperties.instance()));
  }

  @Test
  public void testCreateLakehouse() {
    TrinityLake.createLakehouse(storage, LakehouseDef.newBuilder().build());
    Assertions.assertThat(storage.exists(FileLocations.LATEST_VERSION_HINT_FILE_PATH)).isTrue();
    TreeRoot root = TreeOperations.findLatestRoot(storage);
    Assertions.assertThat(root.previousRootNodeFilePath().isPresent()).isFalse();
    Assertions.assertThat(root.path().get()).isEqualTo(FileLocations.rootNodeFilePath(0));
  }

  @Test
  public void testCreateNamespace() {
    LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).build();
    TrinityLake.createLakehouse(storage, lakehouseDef);
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);

    NamespaceDef ns1Def = NamespaceDef.newBuilder().putProperties("k1", "v1").build();
    transaction = TrinityLake.createNamespace(storage, transaction, "ns1", ns1Def);
    TrinityLake.commitTransaction(storage, transaction);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    Assertions.assertThat(root.path().isPresent()).isTrue();
    Assertions.assertThat(root.path().get()).isEqualTo(FileLocations.rootNodeFilePath(1));
    Assertions.assertThat(root.previousRootNodeFilePath().isPresent()).isTrue();
    Assertions.assertThat(root.previousRootNodeFilePath().get())
        .isEqualTo(FileLocations.rootNodeFilePath(0));
    String ns1Key = ObjectKeys.namespaceKey("ns1", lakehouseDef);
    Optional<String> ns1Path = TreeOperations.searchValue(storage, root, ns1Key);
    Assertions.assertThat(ns1Path.isPresent()).isTrue();
    NamespaceDef readDef = ObjectDefinitions.readNamespaceDef(storage, ns1Path.get());
    Assertions.assertThat(readDef).isEqualTo(ns1Def);
  }

  @Test
  public void testCreateDescribeNamespace() {
    LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).build();
    TrinityLake.createLakehouse(storage, lakehouseDef);
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);

    NamespaceDef ns1Def = NamespaceDef.newBuilder().putProperties("k1", "v1").build();
    transaction = TrinityLake.createNamespace(storage, transaction, "ns1", ns1Def);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.namespaceExists(storage, transaction, "ns1")).isTrue();
    NamespaceDef ns1DefDescribe = TrinityLake.describeNamespace(storage, transaction, "ns1");
    Assertions.assertThat(ns1DefDescribe).isEqualTo(ns1Def);
  }

  @Test
  public void testCreateDescribeAlterDescribeNamespace() {
    LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).build();
    TrinityLake.createLakehouse(storage, lakehouseDef);
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);

    NamespaceDef ns1Def = NamespaceDef.newBuilder().putProperties("k1", "v1").build();
    transaction = TrinityLake.createNamespace(storage, transaction, "ns1", ns1Def);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.namespaceExists(storage, transaction, "ns1")).isTrue();
    NamespaceDef ns1DefDescribe = TrinityLake.describeNamespace(storage, transaction, "ns1");
    Assertions.assertThat(ns1DefDescribe).isEqualTo(ns1Def);

    NamespaceDef ns1DefAlter = NamespaceDef.newBuilder().putProperties("k1", "v2").build();
    transaction = TrinityLake.alterNamespace(storage, transaction, "ns1", ns1DefAlter);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.namespaceExists(storage, transaction, "ns1")).isTrue();
    NamespaceDef ns1DefAlterDescribe = TrinityLake.describeNamespace(storage, transaction, "ns1");
    Assertions.assertThat(ns1DefAlterDescribe).isEqualTo(ns1DefAlter);
  }

  @Test
  public void testCreateDescribeDropDescribeNamespace() {
    LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).build();
    TrinityLake.createLakehouse(storage, lakehouseDef);
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);

    NamespaceDef ns1Def = NamespaceDef.newBuilder().putProperties("k1", "v1").build();
    transaction = TrinityLake.createNamespace(storage, transaction, "ns1", ns1Def);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.namespaceExists(storage, transaction, "ns1")).isTrue();
    NamespaceDef ns1DefDescribe = TrinityLake.describeNamespace(storage, transaction, "ns1");
    Assertions.assertThat(ns1DefDescribe).isEqualTo(ns1Def);

    transaction = TrinityLake.dropNamespace(storage, transaction, "ns1");
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.namespaceExists(storage, transaction, "ns1")).isFalse();
  }
}
