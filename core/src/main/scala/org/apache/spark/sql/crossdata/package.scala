/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

package object crossdata {

  val CrossdataVersion = "1.0.0"

  case class CrossdataTable(tableName: String, dbName: Option[String],  userSpecifiedSchema: Option[StructType], provider: String, partitionColumn: Array[String], opts: Map[String, String] = Map.empty[String, String] , crossdataVersion: String = CrossdataVersion)

  // TODO prop
  val Driver = "crossdata.catalog.mysql.driver"
  val Ip="crossdata.catalog.mysql.ip"
  val Port="crossdata.catalog.mysql.port"
  val Database="crossdata.catalog.mysql.db.name"
  val Table="crossdata.catalog.mysql.db.persistTable"
  val User="crossdata.catalog.mysql.db.user"
  val Pass="crossdata.catalog.mysql.db.pass"

  val StringSeparator: String = "."

}
