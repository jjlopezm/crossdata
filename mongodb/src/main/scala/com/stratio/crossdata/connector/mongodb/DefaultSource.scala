/*
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
package com.stratio.crossdata.connector.mongodb

import com.mongodb.DBCollection
import com.mongodb.casbah.Imports.DBObject
import com.mongodb.casbah.MongoDB

import com.stratio.crossdata.connector.TableInventory.Table
import com.stratio.crossdata.connector.{TableInventory, TableManipulation}
import com.stratio.datasource.mongodb.config.{MongodbConfig, MongodbConfigBuilder}
import com.stratio.datasource.mongodb.{DefaultSource => ProviderDS, MongodbRelation}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Allows creation of MongoDB based tables using
 * the syntax CREATE TEMPORARY TABLE ... USING com.stratio.deep.mongodb.
 * Required options are detailed in [[com.stratio.datasource.mongodb.config.MongodbConfig]]
 */
class DefaultSource extends ProviderDS with TableInventory with DataSourceRegister with TableManipulation{

  import MongodbConfig._

  /**
    * if the collection is capped
    */
  val MongoCollectionPropertyCapped:String= "capped"

  /**
    * collection size
    */
  val MongoCollectionPropertySize:String= "size"

  /**
    * max number of documents
    */
  val MongoCollectionPropertyMax:String= "max"

  override def shortName(): String = "mongodb"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    MongodbXDRelation(
      MongodbConfigBuilder(parseParameters(parameters))
        .build())(sqlContext)

  }

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType): BaseRelation = {

    MongodbXDRelation(
      MongodbConfigBuilder(parseParameters(parameters))
        .build(),Some(schema))(sqlContext)

  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {

    val mongodbRelation = MongodbXDRelation(
      MongodbConfigBuilder(parseParameters(parameters))
        .build())(sqlContext)

    mode match{
      case Append         => mongodbRelation.insert(data, overwrite = false)
      case Overwrite      => mongodbRelation.insert(data, overwrite = true)
      case ErrorIfExists  => if(mongodbRelation.isEmptyCollection) mongodbRelation.insert(data, overwrite = false)
      else throw new UnsupportedOperationException("Writing in a non-empty collection.")
      case Ignore         => if(mongodbRelation.isEmptyCollection) mongodbRelation.insert(data, overwrite = false)
    }

    mongodbRelation
  }


  /**
   * @inheritdoc
   */
  override def generateConnectorOpts(item: Table, userOpts: Map[String, String]): Map[String, String] = Map(
    Database -> item.database.get,
    Collection -> item.tableName
  ) ++ userOpts

  /**
   * @inheritdoc
   */
  override def listTables(context: SQLContext, options: Map[String, String]): Seq[Table] = {

    Seq(Host).foreach { opName =>
      if (!options.contains(opName)) sys.error( s"""Option "$opName" is mandatory for IMPORT TABLES""")
    }


    val hosts: List[String] = options(Host).split(",").toList
    MongodbConnection.withClientDo(hosts) { mongoClient =>

      def extractAllDatabases: Seq[MongoDB] =
        mongoClient.getDatabaseNames().map(mongoClient.getDB)

      def extractAllCollections(db: MongoDB): Seq[DBCollection] =
        db.getCollectionNames().map(db.getCollection).toSeq

      val tablesIt: Iterable[Table] = for {
        database: MongoDB <- extractAllDatabases
        collection: DBCollection <- extractAllCollections(database)
        if options.get(Database).forall( _ == collection.getDB.getName)
        if options.get(Collection).forall(_ == collection.getName)
      } yield {
        collectionToTable(context, options, database.getName, collection.getName)
      }
      tablesIt.toSeq
    }
  }

  //Avoids importing system tables
  override def exclusionFilter(t: TableInventory.Table): Boolean =
    !t.tableName.startsWith("""system.""") && !t.database.get.equals("local")

  private def collectionToTable(context: SQLContext, options: Map[String, String], database: String, collection: String): Table = {

    val collectionConfig = MongodbConfigBuilder()
      .apply(parseParameters(options + (Database -> database) + (Collection -> collection)))
      .build()
    Table(collection, Some(database), Some(new MongodbRelation(collectionConfig)(context).schema))
  }

  override def createExternalTable(context: SQLContext,
                                   tableName: String,
                                   databaseName: Option[String],
                                   schema: StructType,
                                   options: Map[String, String]): Option[Table] = {

    val database: String = options.get(Database).orElse(databaseName).
      getOrElse(throw new RuntimeException(s"$Database required when use CREATE EXTERNAL TABLE command"))

    val collection: String = options.getOrElse(Collection, tableName)

    val mongoOptions = DBObject()
    options.map {
      case (MongoCollectionPropertyCapped, value) => mongoOptions.put(MongoCollectionPropertyCapped, value)
      case (MongoCollectionPropertySize, value) => mongoOptions.put(MongoCollectionPropertySize, value.toInt)
      case (MongoCollectionPropertyMax, value) => mongoOptions.put(MongoCollectionPropertyMax, value.toInt)
      case _ =>
    }

    val hosts: List[String] = options(Host).split(",").toList

    try {

      MongodbConnection.withClientDo(hosts) { mongoClient =>
        mongoClient.getDB(database).createCollection(collection, mongoOptions)
      }
      Option(Table(collection, Option(database), Option(schema)))
    } catch {
      case e: IllegalArgumentException =>
        throw e
      case e: Exception =>
        sys.error(e.getMessage)
        None
    }
  }
}