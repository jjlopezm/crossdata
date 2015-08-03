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

package com.stratio.crossdata.sql.sources

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A BaseRelation that can execute the whole logical plan without running the query
 * on the Spark cluster. If a specific logical plan cannot be resolved by the datasource
 * a None should be returned and the process will be executed on Spark.
 */
@DeveloperApi
trait NativeScan {
  def buildScan(optimizedLogicalPlan: LogicalPlan): Option[Array[Row]]
}
