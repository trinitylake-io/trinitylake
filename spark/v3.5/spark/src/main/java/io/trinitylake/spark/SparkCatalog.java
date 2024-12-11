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

import io.trinitylake.BasicLakehouseVersion;
import io.trinitylake.Lakehouse;
import io.trinitylake.TransactionContext;
import io.trinitylake.exception.NoActiveTransactionException;
import io.trinitylake.exception.TransactionAlreadyExistsException;
import io.trinitylake.spark.source.HasLakeHouse;
import io.trinitylake.spark.source.SupportsTransactions;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkCatalog implements HasLakeHouse, SupportsNamespaces, SupportsTransactions {
  private String catalogName = null;
  private Lakehouse lakeHouse = null;
  private final ThreadLocal<TransactionContext> currentTransaction = new ThreadLocal<>();

  @Override
  public void initialize(String catalogName, CaseInsensitiveStringMap options) {
    this.catalogName = catalogName;
    this.lakeHouse = null;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return null;
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return null;
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return null;
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return false;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {}

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return new String[0][];
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return new String[0][];
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return new HashMap<>();
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {}

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {}

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return false;
  }

  @Override
  public void beginTransaction() {
    if (hasActiveTransaction()) {
      throw new TransactionAlreadyExistsException("Transaction already active in this session.");
    }
    String txnId = generateTransactionId();
    BasicLakehouseVersion version = lakeHouse.beginTransaction(txnId);
    currentTransaction.set(new TransactionContext(txnId, version));
  }

  @Override
  public void commitTransaction() {
    ensureActiveTransaction();
    TransactionContext ctx = currentTransaction.get();
    try {
      lakeHouse.commitTransaction(ctx.transactionId(), ctx.version());
    } finally {
      currentTransaction.remove();
    }
  }

  @Override
  public void rollbackTransaction() {
    ensureActiveTransaction();
    TransactionContext ctx = currentTransaction.get();
    try {
      lakeHouse.rollbackTransaction(ctx.transactionId());
    } finally {
      currentTransaction.remove();
    }
  }

  @Override
  public boolean hasActiveTransaction() {
    return currentTransaction.get() != null;
  }

  @Override
  public Lakehouse lakeHouse() {
    return lakeHouse;
  }

  private String generateTransactionId() {
    return String.valueOf(System.currentTimeMillis());
  }

  private void ensureActiveTransaction() {
    if (!hasActiveTransaction()) {
      throw new NoActiveTransactionException("No active transaction");
    }
  }
}
