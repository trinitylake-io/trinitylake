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
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.storage.LakehouseStorages;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TrinityLakeSparkCatalog implements SupportsNamespaces {

  private static final String DEFAULT_NAMESPACE_OPTION = "default-namespace";

  private String catalogName = null;
  private String[] defaultNamespace = null;
  private SparkSession sparkSession;
  private LakehouseStorage lakehouseStorage;
  private TransactionOptions transactionOptions;
  private Optional<RunningTransaction> globalTransaction;

  public TrinityLakeSparkCatalog() {}

  @Override
  public boolean namespaceExists(String[] namespace) {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();
    return TrinityLake.namespaceExists(lakehouseStorage, transaction, namespaceName);
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    RunningTransaction transaction = currentTransaction();
    List<String> namespaceNames = TrinityLake.showNamespaces(lakehouseStorage, transaction);
    return namespaceNames.stream().map(TrinityLakeToSpark::namespaceName).toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Listing nested namespace is not supported");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();
    NamespaceDef namespaceDef =
        TrinityLake.describeNamespace(lakehouseStorage, transaction, namespaceName);
    return namespaceDef.getPropertiesMap();
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> properties)
      throws NamespaceAlreadyExistsException {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();
    NamespaceDef namespaceDef = NamespaceDef.newBuilder().putAllProperties(properties).build();
    TrinityLake.createNamespace(lakehouseStorage, transaction, namespaceName, namespaceDef);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... namespaceChanges)
      throws NoSuchNamespaceException {
    String namespaceName = SparkToTrinityLake.namespaceName(namespace);
    RunningTransaction transaction = currentTransaction();
    NamespaceDef namespaceDef =
        TrinityLake.describeNamespace(lakehouseStorage, transaction, namespaceName);

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

    TrinityLake.alterNamespace(lakehouseStorage, transaction, namespaceName, namespaceDef);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    if (cascade) {
      throw new UnsupportedOperationException("Cannot drop namespace when cascade is enabled");
    }

    RunningTransaction transaction = currentTransaction();

    TrinityLake.dropNamespace(
        lakehouseStorage, transaction, SparkToTrinityLake.namespaceName(namespace));
    return true;
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

    this.lakehouseStorage = LakehouseStorages.initialize(options);
    this.transactionOptions = new TransactionOptions(options);
  }

  public LakehouseStorage lakehouseStorage() {
    return lakehouseStorage;
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
        () -> TrinityLake.beginTransaction(lakehouseStorage, transactionOptions.asStringMap()));
  }
}
