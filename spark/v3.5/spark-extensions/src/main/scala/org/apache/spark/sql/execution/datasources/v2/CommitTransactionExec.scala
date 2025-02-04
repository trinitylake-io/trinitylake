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
package org.apache.spark.sql.execution.datasources.v2

import io.trinitylake.TrinityLake
import io.trinitylake.spark.TrinityLakeSparkCatalog
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

case class CommitTransactionExec() extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    session.sessionState.catalogManager.currentCatalog match {
      case catalog: TrinityLakeSparkCatalog => {
        if (!catalog.globalTransaction().isPresent) {
          throw new IllegalStateException("No transaction to be committed")
        }
        try {
          TrinityLake.commitTransaction(catalog.lakehouseStorage, catalog.globalTransaction.get)
        } finally {
          // regardless of successful or not, the running transaction will be cleared,
          // because failed transaction commit cannot be retried
          catalog.clearGlobalTransaction()
        }
      }
      case _ =>
        throw new UnsupportedOperationException(
          "Cannot begin transaction in non-TrinityLake catalog")
    }
    Seq.empty
  }

  override def simpleString(maxFields: Int): String = {
    "CommitTransactionExec"
  }
}
