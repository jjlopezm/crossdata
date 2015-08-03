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

import java.lang.reflect.Constructor
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.catalyst.{CatalystConf, SimpleCatalystConf}
import org.apache.spark.sql.sources.{LogicalRelation, RelationProvider, CreatableRelationProvider}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.cassandra._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkContext}

/**
 * CrossdataContext leverages the features of [[org.apache.spark.sql.SQLContext]]
 * and adds some features of the Crossdata system.
 * @param sc A [[org.apache.spark.SparkContext]].
 */
class XDContext(sc: SparkContext) extends SQLContext(sc) with Logging {

  val xdConfig: Config = ConfigFactory.load
  val catalogClass: String = xdConfig.getString("crossdata.catalog.class")
  val caseSensitive: Boolean = xdConfig.getBoolean("crossdata.catalog.caseSensitive")

  import scala.collection.JavaConversions._

  val catalogArgs: util.List[String] =
    xdConfig.getList("crossdata.catalog.args").map(e => e.toString)

  val xdCatalog = Class.forName(catalogClass)

  val constr: Constructor[_] = xdCatalog.getConstructor(
    classOf[CatalystConf],
    classOf[util.List[String]])

  override protected[sql] lazy val catalog: XDCatalog =
    constr.newInstance(
      new SimpleCatalystConf(caseSensitive),
      catalogArgs).asInstanceOf[XDCatalog]

  catalog.open()

  override def sql(sqlText: String): DataFrame = {
    XDDataframe(this, parseSql(sqlText))
  }

  def persistTable(tableName: String, clusterName: String, cols: Seq[StructField]) = {

    val loader = Utils.getContextOrSparkClassLoader
    clusterName match {
      case "Cassandra"=> {
        val provider = "com.stratio.crossdata.sql.sources.cassandra"
        val identifier:Seq[String]=List(clusterName, tableName)
        val clazz=loader.loadClass(provider + ".DefaultSource")

        val relation = clazz.newInstance() match {
          case dataSource: RelationProvider =>
            val params:Map[String, String]=Map()
            dataSource.createRelation(this, params)
          case _ => sys.error(s"${clazz.getCanonicalName} does not allow create table.")
        }
        catalog.registerTable(identifier,LogicalRelation(relation))
      }
      case _ =>
    }
  }

}

