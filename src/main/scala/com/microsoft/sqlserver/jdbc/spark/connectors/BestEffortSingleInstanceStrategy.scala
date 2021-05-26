/**
* Copyright 2020 and onwards Microsoft Corporation
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.microsoft.sqlserver.jdbc.spark

import java.sql.ResultSetMetaData

import com.microsoft.sqlserver.jdbc.spark.BulkCopyUtils.{savePartition}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Implements the BEST_EFFORT write strategy for Single Instance. All executors insert
 * into a user specified table directly. Write to table is not transactional and
 * may results in duplicates in executor restart scenarios.
 */

object SingleInstanceWriteStrategies extends DataIOStrategy with Logging {
  /**
   * write implements the default write strategy. Each executor writes a
   * partition of data to SQL instance. The executors will retry under failures
   * and this may result in duplicates
   */
  def write(
         df: DataFrame,
         colMetaData: Array[ColumnMetadata],
         options: SQLServerBulkJdbcOptions,
         appId: String): Unit = {
      logInfo("write : best effort write to single instance called")
      //val dfColMetadata: Array[ColumnMetadata] = getColMetadataMap(metadata)
      val dfColMetadata = colMetaData
      df.rdd.foreachPartition(iterator =>
        savePartition(iterator, options.dbtable, dfColMetadata, options))
    }
}