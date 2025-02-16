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

import io.trinitylake.models.Column;
import io.trinitylake.models.DataType;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.Schema;
import io.trinitylake.models.TableDef;
import io.trinitylake.relocated.com.google.common.collect.ImmutableMap;
import io.trinitylake.storage.BasicLakehouseStorage;
import io.trinitylake.storage.CommonStorageOpsProperties;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LiteralURI;
import io.trinitylake.storage.local.LocalStorageOps;
import io.trinitylake.storage.local.LocalStorageOpsProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

public class TestObjectDefinitions {

    @TempDir
    private File tempDir;
    private LakehouseStorage storage;

    final private LakehouseDef lakehouseDef = LakehouseDef.newBuilder().setMajorVersion(2)
            .setMinimumVersionsToKeep(5)
            .setTableNameMaxSizeBytes(256)
            .setNamespaceNameMaxSizeBytes(64).build();
    final private NamespaceDef namespaceDef = NamespaceDef.newBuilder()
            .putProperties("k1", "v1").build();
    final private String namespaceName = "test-namespace";
    final private TableDef tableDef = TableDef.newBuilder()
            .setSchema(Schema.newBuilder()
                    .addColumns(Column.newBuilder().setName("col1").setType(DataType.VARCHAR).build()))
            .putProperties("k1", "v1").build();
    final private String tableName = "test-table";

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
    public void testWriteLakehouseDef() {
        String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
        File lakehouseDefFile = new File(tempDir, lakehouseDefFilePath);

        Assertions.assertFalse(lakehouseDefFile.exists());
        ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);
        Assertions.assertTrue(lakehouseDefFile.exists());
    }

    @Test
    public void testReadLakehouseDef() {
        String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
        ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);

        LakehouseDef testLakehouseDef = ObjectDefinitions.readLakehouseDef(storage, lakehouseDefFilePath);
        Assertions.assertEquals(lakehouseDef, testLakehouseDef);
    }

    @Test
    public void testWriteNamespaceDef() {
        String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
        ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);
        String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);

        File namespaceDefFile = new File(tempDir, namespaceDefFilePath);
        Assertions.assertFalse(namespaceDefFile.exists());
        ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
        Assertions.assertTrue(namespaceDefFile.exists());
    }

    @Test
    public void testReadNamespaceDef() {
        String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
        ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);
        String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
        ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);

        NamespaceDef testNamespaceDef = ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath);
        Assertions.assertEquals(namespaceDef, testNamespaceDef);
    }

    @Test
    public void testWriteTableDef() {
        String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
        ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);
        String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
        ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
        String tableDefFilePath = FileLocations.newTableDefFilePath(namespaceName, tableName);
        File tableDefFile = new File(tempDir, tableDefFilePath);
        Assertions.assertFalse(tableDefFile.exists());
        ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);
        Assertions.assertTrue(tableDefFile.exists());
    }

    @Test
    public void testReadTableDef() {
        String lakehouseDefFilePath = FileLocations.newLakehouseDefFilePath();
        ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);
        String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
        ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
        String tableDefFilePath = FileLocations.newTableDefFilePath(namespaceName, tableName);
        ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);

        TableDef testTableDef = ObjectDefinitions.readTableDef(storage, tableDefFilePath);
        Assertions.assertEquals(tableDef, testTableDef);
    }
}
