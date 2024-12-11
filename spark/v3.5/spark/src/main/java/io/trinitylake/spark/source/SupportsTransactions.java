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
package io.trinitylake.spark.source;

import io.trinitylake.exception.CommitFailureException;
import io.trinitylake.exception.NoActiveTransactionException;
import io.trinitylake.exception.TransactionAlreadyExistsException;

public interface SupportsTransactions {

  /**
   * Begin a new transaction.
   *
   * <p>This operation will fail if there is already an active transaction in the current session. A
   * transaction must be started before performing any transactional operations.
   *
   * @throws TransactionAlreadyExistsException if a transaction is already active
   */
  void beginTransaction();

  /**
   * Commit the current transaction.
   *
   * <p>This operation persists all changes made during the transaction and makes them visible to
   * other sessions. The transaction must be active when committing.
   *
   * @throws NoActiveTransactionException if no transaction is active
   * @throws CommitFailureException if the commit operation fails
   */
  void commitTransaction();

  /**
   * Rollback the current transaction.
   *
   * <p>This operation discards all changes made during the transaction and releases any locks. The
   * transaction must be active when rolling back.
   *
   * @throws NoActiveTransactionException if no transaction is active
   */
  void rollbackTransaction();

  /**
   * Check if there is currently an active transaction in this session.
   *
   * @return true if there is an active transaction, false otherwise
   */
  boolean hasActiveTransaction();
}
