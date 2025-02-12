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

import io.trinitylake.models.*;
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

public class TestTrinityLakeTableOps {

  private static final LakehouseDef LAKEHOUSE_DEF =
      LakehouseDef.newBuilder().setNamespaceNameMaxSizeBytes(8).setTableNameMaxSizeBytes(8).build();
  private static final String NS1 = "ns1";
  private static final NamespaceDef NS1_DEF =
      NamespaceDef.newBuilder().putProperties("k1", "v1").build();

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

    TrinityLake.createLakehouse(storage, LAKEHOUSE_DEF);
    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createNamespace(storage, transaction, NS1, NS1_DEF);
    TrinityLake.commitTransaction(storage, transaction);
  }

  @Test
  public void testCreateTable() {
    TableDef tableDef =
        TableDef.newBuilder()
            .setSchema(
                Schema.newBuilder()
                    .addColumns(Column.newBuilder().setName("c1").setType(DataType.VARCHAR).build())
                    .build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NS1, "t1", tableDef);
    TrinityLake.commitTransaction(storage, transaction);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    Assertions.assertThat(root.path().isPresent()).isTrue();
    Assertions.assertThat(root.path().get()).isEqualTo(FileLocations.rootNodeFilePath(2));
    Assertions.assertThat(root.previousRootNodeFilePath().isPresent()).isTrue();
    Assertions.assertThat(root.previousRootNodeFilePath().get())
        .isEqualTo(FileLocations.rootNodeFilePath(1));
    String t1Key = ObjectKeys.tableKey("ns1", "t1", LAKEHOUSE_DEF);
    Optional<String> t1Path = TreeOperations.searchValue(storage, root, t1Key);
    Assertions.assertThat(t1Path.isPresent()).isTrue();
    TableDef readDef = ObjectDefinitions.readTableDef(storage, t1Path.get());
    Assertions.assertThat(readDef).isEqualTo(tableDef);
  }

  @Test
  public void testCreateDescribeTable() {
    TableDef tableDef =
        TableDef.newBuilder()
            .setSchema(
                Schema.newBuilder()
                    .addColumns(Column.newBuilder().setName("c1").setType(DataType.VARCHAR).build())
                    .build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NS1, "t1", tableDef);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.tableExists(storage, transaction, "ns1", "t1")).isTrue();
    TableDef t1DefDescribe = TrinityLake.describeTable(storage, transaction, "ns1", "t1");
    Assertions.assertThat(t1DefDescribe).isEqualTo(tableDef);
  }

  @Test
  public void testCreateDescribeAlterDescribeTable() {
    TableDef tableDef =
        TableDef.newBuilder()
            .setSchema(
                Schema.newBuilder()
                    .addColumns(Column.newBuilder().setName("c1").setType(DataType.VARCHAR).build())
                    .build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NS1, "t1", tableDef);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.tableExists(storage, transaction, "ns1", "t1")).isTrue();
    TableDef t1DefDescribe = TrinityLake.describeTable(storage, transaction, "ns1", "t1");
    Assertions.assertThat(t1DefDescribe).isEqualTo(tableDef);

    TableDef t1DefAlter =
        TableDef.newBuilder()
            .setSchema(
                Schema.newBuilder()
                    .addColumns(Column.newBuilder().setName("c1").setType(DataType.VARCHAR).build())
                    .addColumns(Column.newBuilder().setName("c2").setType(DataType.VARCHAR).build())
                    .build())
            .putProperties("k1", "v2")
            .build();
    transaction = TrinityLake.alterTable(storage, transaction, "ns1", "t1", t1DefAlter);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.tableExists(storage, transaction, "ns1", "t1")).isTrue();
    TableDef t1DefAlterDescribe = TrinityLake.describeTable(storage, transaction, "ns1", "t1");
    Assertions.assertThat(t1DefAlterDescribe).isEqualTo(t1DefAlter);
  }

  @Test
  public void testCreateDescribeDropDescribeNamespace() {
    TableDef tableDef =
        TableDef.newBuilder()
            .setSchema(
                Schema.newBuilder()
                    .addColumns(Column.newBuilder().setName("c1").setType(DataType.VARCHAR).build())
                    .build())
            .putProperties("k1", "v1")
            .build();

    RunningTransaction transaction = TrinityLake.beginTransaction(storage);
    transaction = TrinityLake.createTable(storage, transaction, NS1, "t1", tableDef);
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.tableExists(storage, transaction, "ns1", "t1")).isTrue();
    TableDef t1DefDescribe = TrinityLake.describeTable(storage, transaction, "ns1", "t1");
    Assertions.assertThat(t1DefDescribe).isEqualTo(tableDef);

    transaction = TrinityLake.dropTable(storage, transaction, "ns1", "t1");
    TrinityLake.commitTransaction(storage, transaction);

    transaction = TrinityLake.beginTransaction(storage);
    Assertions.assertThat(TrinityLake.tableExists(storage, transaction, "ns1", "t1")).isFalse();
  }
}
