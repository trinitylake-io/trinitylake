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
package io.trinitylake.spark;

import io.trinitylake.RunningTransaction;
import io.trinitylake.TransactionOptions;
import io.trinitylake.TrinityLake;
import io.trinitylake.exception.ObjectAlreadyExistsException;
import io.trinitylake.exception.ObjectNotFoundException;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LakehouseStorages;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalogCapability;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TrinityLakeSparkCatalog implements StagingTableCatalog, SupportsNamespaces {

  private static final String DEFAULT_NAMESPACE_OPTION = "default-namespace";

  private String catalogName = null;
  private String[] defaultNamespace = null;
  private SparkSession sparkSession;
  private LakehouseStorage storage;
  private TransactionOptions transactionOptions;
  private Optional<RunningTransaction> globalTransaction;

  public TrinityLakeSparkCatalog() {}

  @Override
  public boolean namespaceExists(String[] namespace) {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();
    return TrinityLake.namespaceExists(storage, transaction, namespaceName);
  }

  @Override
  public String[][] listNamespaces() {
    RunningTransaction transaction = currentTransaction();
    List<String> namespaceNames = TrinityLake.showNamespaces(storage, transaction);
    return namespaceNames.stream().map(TrinityLakeToSpark::namespaceName).toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    // TODO: this could be represented by users supplying a special character,
    //   for example a $ sign, such that a multi-level namesapce ns1.ns2 is represented as ns1$ns2
    // in TrinityLake
    throw new UnsupportedOperationException("Nested namespace is not supported in TrinityLake");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();
    try {
      NamespaceDef namespaceDef =
          TrinityLake.describeNamespace(storage, transaction, namespaceName);
      return namespaceDef.getPropertiesMap();
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> properties)
      throws NamespaceAlreadyExistsException {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();
    NamespaceDef namespaceDef = NamespaceDef.newBuilder().putAllProperties(properties).build();
    try {
      TrinityLake.createNamespace(storage, transaction, namespaceName, namespaceDef);
    } catch (ObjectAlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... namespaceChanges)
      throws NoSuchNamespaceException {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();

    NamespaceDef namespaceDef;
    try {
      namespaceDef = TrinityLake.describeNamespace(storage, transaction, namespaceName);
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException(namespace);
    }

    NamespaceDef.Builder namespaceDefBuilder = namespaceDef.toBuilder();
    for (NamespaceChange namespaceChange : namespaceChanges) {
      if (namespaceChange instanceof NamespaceChange.RemoveProperty) {
        NamespaceChange.RemoveProperty removeProperty =
            (NamespaceChange.RemoveProperty) namespaceChange;
        namespaceDefBuilder.removeProperties(removeProperty.property());
      }

      if (namespaceChange instanceof NamespaceChange.SetProperty) {
        NamespaceChange.SetProperty setProperty = (NamespaceChange.SetProperty) namespaceChange;
        namespaceDefBuilder.putProperties(setProperty.property(), setProperty.value());
      }
    }

    TrinityLake.alterNamespace(storage, transaction, namespaceName, namespaceDef);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();

    try {
      transaction = TrinityLake.dropNamespace(storage, transaction, namespaceName);
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException(namespace);
    }

    // TODO: move this to the core library
    if (cascade) {
      List<String> tableNames = TrinityLake.showTables(storage, transaction, namespaceName);
      for (String tableName : tableNames) {
        transaction = TrinityLake.dropTable(storage, transaction, namespaceName, tableName);
      }
    }

    TrinityLake.commitTransaction(storage, transaction);

    return true;
  }

  @Override
  public boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
    return StagingTableCatalog.super.purgeTable(ident);
  }

  @Override
  public boolean useNullableQuerySchema() {
    return StagingTableCatalog.super.useNullableQuerySchema();
  }

  @Override
  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return StagingTableCatalog.super.createTable(ident, columns, partitions, properties);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return StagingTableCatalog.super.tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return null;
  }

  @Override
  public void invalidateTable(Identifier ident) {
    StagingTableCatalog.super.invalidateTable(ident);
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return StagingTableCatalog.super.loadTable(ident, timestamp);
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return StagingTableCatalog.super.loadTable(ident, version);
  }

  @Override
  public Set<TableCatalogCapability> capabilities() {
    return StagingTableCatalog.super.capabilities();
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {}

  @Override
  public boolean dropTable(Identifier ident) {
    return false;
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return null;
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return null;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException {
    return StagingTableCatalog.super.stageCreateOrReplace(ident, columns, partitions, properties);
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    return StagingTableCatalog.super.stageReplace(ident, columns, partitions, properties);
  }

  @Override
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return null;
  }

  @Override
  public StagedTable stageCreate(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return StagingTableCatalog.super.stageCreate(ident, columns, partitions, properties);
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    return null;
  }

  @Override
  public String[] defaultNamespace() {
    return defaultNamespace;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.sparkSession = SparkSession.active();

    if (options.containsKey(DEFAULT_NAMESPACE_OPTION)) {
      this.defaultNamespace = new String[] {options.get(DEFAULT_NAMESPACE_OPTION)};
    }

    this.storage = LakehouseStorages.initialize(options);
    this.transactionOptions = new TransactionOptions(options);
  }

  public LakehouseStorage lakehouseStorage() {
    return storage;
  }

  public TransactionOptions transactionOptions() {
    return transactionOptions;
  }

  public void setGlobalTransaction(RunningTransaction globalTransaction) {
    this.globalTransaction = Optional.of(globalTransaction);
  }

  public Optional<RunningTransaction> globalTransaction() {
    return globalTransaction;
  }

  public void clearGlobalTransaction() {
    this.globalTransaction = Optional.empty();
  }

  private RunningTransaction currentTransaction() {
    return globalTransaction.orElseGet(
        () -> TrinityLake.beginTransaction(storage, transactionOptions.asStringMap()));
  }
}
