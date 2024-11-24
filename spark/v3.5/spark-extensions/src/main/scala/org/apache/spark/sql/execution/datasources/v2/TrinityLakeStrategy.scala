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

import org.apache.spark.sql.catalyst.plans.logical.BeginTransactionCommand
import org.apache.spark.sql.catalyst.plans.logical.CommitTransactionCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RollbackTransactionCommand
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}

case class TrinityLakeStrategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case BeginTransactionCommand() =>
      BeginTransactionExec() :: Nil
    case CommitTransactionCommand() =>
      CommitTransactionExec() :: Nil
    case RollbackTransactionCommand() =>
      RollbackTransactionExec() :: Nil
    case _ => Nil
  }
}
