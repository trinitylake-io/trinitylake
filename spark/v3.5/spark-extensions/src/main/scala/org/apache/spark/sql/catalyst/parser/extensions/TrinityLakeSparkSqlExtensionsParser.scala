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

import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CodePointCharStream
import org.antlr.v4.runtime.CommonToken
import org.antlr.v4.runtime.IntStream
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.VariableSubstitution
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType

import java.util.Locale
import scala.util.Try

class TrinityLakeSparkSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface {

  import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeSparkSqlExtensionsParser._

  private lazy val astBuilder = new TrinityLakeSqlExtensionsAstBuilder(delegate)
  private lazy val substitutor = {
    Try(substitutorCtor.newInstance(SQLConf.get))
      .getOrElse(substitutorCtor.newInstance())
  }

  /**
   * Parse a string to a LogicalPlan.
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
    if (isTrinityLakeCommand(sqlTextAfterSubstitution)) {
      parse(sqlTextAfterSubstitution) { parser =>
        astBuilder.visit(parser.singleStatement())
      }.asInstanceOf[LogicalPlan]
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  /**
   * Parse a string to an Expression.
   */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /**
   * Parse a string to a TableIdentifier.
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /**
   * Parse a string to a FunctionIdentifier.
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /**
   * Parse a string to a multi-part identifier.
   */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    parsePlan(sqlText)
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  /**
   * Parse a string to a DataType.
   */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  private def isTrinityLakeCommand(sqlText: String): Boolean = {
    val normalized = sqlText
      .toLowerCase(Locale.ROOT)
      .trim()
      // Strip simple SQL comments that terminate a line, e.g. comments starting with `--` .
      .replaceAll("--.*?\\n", " ")
      // Strip newlines.
      .replaceAll("\\s+", " ")
      // Strip comments of the form  /* ... */. This must come after stripping newlines so that
      // comments that span multiple lines are caught.
      .replaceAll("/\\*.*?\\*/", " ")
      // Strip backtick then `system`.`ancestors_of` changes to system.ancestors_of
      .replaceAll("`", "")
      .trim()

    normalized.startsWith("begin") ||
    normalized.startsWith("commit") ||
    normalized.startsWith("rollback")
  }

  protected def parse[T](command: String)(toResult: TrinityLakeSqlExtensionsParser => T): T = {
    val lexer = new TrinityLakeSqlExtensionsLexer(
      new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(TrinityLakeParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new TrinityLakeSqlExtensionsParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(TrinityLakeParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case _: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: TrinityLakeParseException if e.command.isDefined =>
        throw e
      case e: TrinityLakeParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new TrinityLakeParseException(Option(command), e.message, position, position)
    }
  }
}

class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()
  override def getSourceName: String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

case object TrinityLakeParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    throw new TrinityLakeParseException(None, msg, start, stop)
  }
}

class TrinityLakeParseException(
    val command: Option[String],
    message: String,
    val start: Origin,
    val stop: Origin)
    extends AnalysisException(message, start.line, start.startPosition) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(
      Option(TrinityLakeParserUtils.command(ctx)),
      message,
      TrinityLakeParserUtils.position(ctx.getStart),
      TrinityLakeParserUtils.position(ctx.getStop))
  }

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(
            Some(l),
            Some(p),
            Some(startIndex),
            Some(stopIndex),
            Some(sqlText),
            Some(objectType),
            Some(objectName)) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): TrinityLakeParseException = {
    new TrinityLakeParseException(Option(cmd), message, start, stop)
  }
}

object TrinityLakeSparkSqlExtensionsParser {
  private val substitutorCtor = {
    Try(classOf[VariableSubstitution].getConstructor(classOf[SQLConf]))
      .getOrElse(classOf[VariableSubstitution].getConstructor())
  }
}
