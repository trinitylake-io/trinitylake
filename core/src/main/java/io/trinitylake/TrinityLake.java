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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.trinitylake.exception.CommitFailureException;
import io.trinitylake.exception.ObjectAlreadyExistException;
import io.trinitylake.exception.ObjectNotFoundException;
import io.trinitylake.models.LakehouseDef;
import io.trinitylake.models.NamespaceDef;
import io.trinitylake.models.TableDef;
import io.trinitylake.storage.FilePaths;
import io.trinitylake.storage.LakehouseStorage;
import io.trinitylake.tree.BasicTreeNode;
import io.trinitylake.tree.TreeKeys;
import io.trinitylake.tree.TreeNode;
import io.trinitylake.tree.TreeOperations;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class TrinityLake {

  public static void createLakehouse(LakehouseStorage storage, LakehouseDef lakehouseDef) {
    String lakehouseDefFilePath = FilePaths.newLakehouseDefFilePath();
    ObjectDefinitions.writeLakehouseDef(storage, lakehouseDefFilePath, lakehouseDef);

    BasicTreeNode root = new BasicTreeNode();
    root.set(TreeKeys.LAKEHOUSE_DEFINITION, lakehouseDefFilePath);
    root.set(TreeKeys.VERSION, Long.toString(0));
    String rootNodeFilePath = FilePaths.rootNodeFilePath(0);
    TreeOperations.writeNodeFile(storage, rootNodeFilePath, root);
  }

  public static RunningTransaction beginTransaction(LakehouseStorage storage) {
    return beginTransaction(storage, ImmutableMap.of());
  }

  public static RunningTransaction beginTransaction(
      LakehouseStorage storage, Map<String, String> options) {
    TreeNode current = TreeOperations.findLatestRoot(storage);
    TransactionOptions transactionOptions = new TransactionOptions(options);
    return ImmutableRunningTransaction.builder()
        .beganAtMillis(System.currentTimeMillis())
        .transactionId(UUID.randomUUID().toString())
        .beginningRoot(current)
        .runningRoot(current)
        .isolationLevel(transactionOptions.isolationLevel())
        .build();
  }

  public static CommittedTransaction commitTransaction(
      LakehouseStorage storage, RunningTransaction transaction) throws CommitFailureException {
    Preconditions.checkArgument(
        TreeOperations.hasVersion(transaction.runningRoot()), "There is no change to be committed");
    long beginningRootVersion = TreeOperations.findVersion(transaction.beginningRoot());
    String nextVersionFilePath = FilePaths.rootNodeFilePath(beginningRootVersion + 1);
    TreeOperations.writeNodeFile(storage, nextVersionFilePath, transaction.runningRoot());
    return ImmutableCommittedTransaction.builder()
        .committedRoot(transaction.runningRoot())
        .transactionId(transaction.transactionId())
        .build();
  }

  public static List<String> showNamespaces(
      LakehouseStorage storage, RunningTransaction transaction) {
    return transaction.runningRoot().allKeyValues().stream()
        .map(Map.Entry::getKey)
        .filter(TreeKeys::isNamespaceKey)
        .map(TreeKeys::namespaceNameFromKey)
        .collect(Collectors.toList());
  }

  public static boolean namespaceExists(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    return transaction.runningRoot().contains(namespaceKey);
  }

  public static NamespaceDef describeNamespace(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName)
      throws ObjectNotFoundException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String namespaceDefFilePath = transaction.runningRoot().get(namespaceKey);
    return ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath);
  }

  public static RunningTransaction createNamespace(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      NamespaceDef namespaceDef)
      throws ObjectAlreadyExistException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectAlreadyExistException("Namespace %s already exists", namespaceName);
    }

    String namespaceDefFilePath = FilePaths.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
    TreeNode newRoot = TreeOperations.clone(transaction.runningRoot());
    newRoot.set(namespaceKey, namespaceDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction alterNamespace(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      NamespaceDef namespaceDef)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    String namespaceDefFilePath = FilePaths.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
    TreeNode newRoot = TreeOperations.clone(transaction.runningRoot());
    newRoot.set(namespaceKey, namespaceDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction dropNamespace(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    TreeNode newRoot = TreeOperations.clone(transaction.runningRoot());
    newRoot.remove(namespaceKey);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static List<String> showTables(
      LakehouseStorage storage, RunningTransaction transaction, String namespaceName)
      throws ObjectNotFoundException {
    return transaction.runningRoot().allKeyValues().stream()
        .map(Map.Entry::getKey)
        .filter(TreeKeys::isTableKey)
        .map(key -> TreeKeys.tableNameFromKey(namespaceName, key))
        .collect(Collectors.toList());
  }

  public static boolean tableExists(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName) {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = TreeKeys.tableNameFromKey(namespaceName, tableName);
    if (!transaction.runningRoot().contains(tableKey)) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exist", namespaceName, tableName);
    }
    return transaction.runningRoot().contains(tableKey);
  }

  public static TableDef describeTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName)
      throws ObjectNotFoundException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = TreeKeys.tableNameFromKey(namespaceName, tableName);
    if (!transaction.runningRoot().contains(tableKey)) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exist", namespaceName, tableName);
    }
    String tableDefFilePath = transaction.runningRoot().get(tableKey);
    return ObjectDefinitions.readTableDef(storage, tableDefFilePath);
  }

  public static RunningTransaction createTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName,
      TableDef tableDef)
      throws ObjectAlreadyExistException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = TreeKeys.tableNameFromKey(namespaceName, tableName);
    if (transaction.runningRoot().contains(tableKey)) {
      throw new ObjectAlreadyExistException(
          "Namespace %s table %s already exists", namespaceName, tableName);
    }

    String tableDefFilePath = FilePaths.newTableDefFilePath(namespaceName, tableName);
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);
    TreeNode newRoot = TreeOperations.clone(transaction.runningRoot());
    newRoot.set(tableKey, tableDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction alterTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName,
      TableDef tableDef)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = TreeKeys.tableNameFromKey(namespaceName, tableName);
    if (!transaction.runningRoot().contains(tableKey)) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exists", namespaceName, tableName);
    }

    String tableDefFilePath = FilePaths.newTableDefFilePath(namespaceName, tableName);
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);
    TreeNode newRoot = TreeOperations.clone(transaction.runningRoot());
    newRoot.set(tableKey, tableDefFilePath);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }

  public static RunningTransaction dropTable(
      LakehouseStorage storage,
      RunningTransaction transaction,
      String namespaceName,
      String tableName)
      throws ObjectNotFoundException, CommitFailureException {
    LakehouseDef lakehouseDef = TreeOperations.findLakehouseDef(storage, transaction.runningRoot());
    String namespaceKey = TreeKeys.namespaceKey(namespaceName, lakehouseDef);
    if (!transaction.runningRoot().contains(namespaceKey)) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = TreeKeys.tableNameFromKey(namespaceName, tableName);
    if (!transaction.runningRoot().contains(tableKey)) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exists", namespaceName, tableName);
    }

    TreeNode newRoot = TreeOperations.clone(transaction.runningRoot());
    newRoot.remove(tableKey);
    return ImmutableRunningTransaction.builder().from(transaction).runningRoot(newRoot).build();
  }
}
