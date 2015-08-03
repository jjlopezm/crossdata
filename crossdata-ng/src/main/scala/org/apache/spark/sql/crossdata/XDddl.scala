/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.sql.crossdata

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.crossdata.execution.XDCommands.CreateTableCommand
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types.{DataTypeParser, StructField}
import org.apache.spark.sql.crossdata.execution._


private[sql] class XDddl(fallback: String => LogicalPlan) extends AbstractSparkSQLParser with DataTypeParser with Logging {

  protected val CREATE = Keyword("CREATE")
  protected val TABLE = Keyword("TABLE")
  protected val ON = Keyword("ON")
  protected val CLUSTER = Keyword("CLUSTER")
  protected val PRIMARY = Keyword("PRIMARY")
  protected val KEY = Keyword("KEY")
  protected val INSERT = Keyword("INSERT")
  protected val INTO = Keyword("INTO")
  protected val VALUES = Keyword("VALUES")

  protected lazy val tableCols: Parser[Seq[StructField]] = "(" ~> repsep(column, ",") <~ ")"
  protected lazy val tableColsIdentifiers: Parser[Seq[String]] = "(" ~> (ident <~ ",".*).* <~ ")"


  protected lazy val column: Parser[StructField] =
    ident ~ dataType ^^ {
      case columnName ~ typ =>
        StructField(columnName, typ, nullable = true)
    }

  override protected lazy val start: Parser[LogicalPlan] = create | insert | others

  private lazy val create: Parser[LogicalPlan] =
    (CREATE ~> TABLE ~> ident) ~ (ON ~> CLUSTER ~> ident) ~ tableCols ^^^ {

      case tableName ~ clusterName ~ cols =>
      CreateTableCommand(tableName.asInstanceOf[String],clusterName.asInstanceOf[String],cols.asInstanceOf[Seq[StructField]])
    }


  private lazy val insert: Parser[LogicalPlan] =
    (INSERT ~> INTO ~> ident) ~ tableColsIdentifiers ~ (VALUES ~> tableColsIdentifiers) ^^^ {
      case tableName ~ cols ~ values =>
      //InsertIntoCommand(tableName, cols, values)
    }

  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }


  case class CreateTableCommand(tableName: String, clusterName: String, cols: Seq[StructField])
    extends RunnableCommand {

    // Run through the optimizer to generate the physical plan.
    override def run(sqlContext: SQLContext): Seq[Row] = try {

      val XDContext = sqlContext.asInstanceOf[XDContext]
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      if (XDContext.catalog.tableExists(Seq(clusterName, tableName))) {
        throw new AnalysisException(s"$clusterName.$tableName already exists.")
      } else {
        XDContext.persistTable(tableName,clusterName,cols)
      }

      Seq.empty[Row]

    } catch {
      case cause: TreeNodeException[_] =>
        ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
    }
  }

}
