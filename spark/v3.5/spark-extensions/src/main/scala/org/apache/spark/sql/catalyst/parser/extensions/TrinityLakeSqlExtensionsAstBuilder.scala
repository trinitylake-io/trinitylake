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
package org.apache.spark.sql.catalyst.parser.extensions

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeSqlExtensionsParser.BeginStatementContext
import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeSqlExtensionsParser.CommitStatementContext
import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeSqlExtensionsParser.RollbackStatementContext
import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeSqlExtensionsParser.SingleStatementContext
import org.apache.spark.sql.catalyst.plans.logical.BeginTransactionCommand
import org.apache.spark.sql.catalyst.plans.logical.CommitTransactionCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RollbackTransactionCommand
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.Origin

class TrinityLakeSqlExtensionsAstBuilder(delegate: ParserInterface)
    extends TrinityLakeSqlExtensionsBaseVisitor[AnyRef] {

  override def visitBeginStatement(ctx: BeginStatementContext): LogicalPlan = {
    BeginTransactionCommand()
  }

  override def visitCommitStatement(ctx: CommitStatementContext): LogicalPlan = {
    CommitTransactionCommand()
  }

  override def visitRollbackStatement(ctx: RollbackStatementContext): LogicalPlan = {
    RollbackTransactionCommand()
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }
}

object TrinityLakeParserUtils {

  private[sql] def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  private[sql] def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /** Get the command which created the token. */
  private[sql] def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
}
