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

import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import com.microsoft.sqlserver.jdbc.spark.BulkCopyUtils._

/**
 * SingleInstanceConnector implements the MsSQLSparkConnector to write to Single SQL Instance.
 * This connector supports both 'BEST_EFFORT' and 'NO_DUPLICATES' modes. The mode is selected based on
 * 'reliabilityLevel' option. The defaut is 'BEST_EFFORT'. Based on the mode the  right write strategy is called.
 */
object SingleInstanceConnector extends Connector with Logging {
  override def writeInParallel(
                 df: DataFrame,
                 colMetaData: Array[ColumnMetadata],
                 options: SQLServerBulkJdbcOptions,
                 appId: String): Unit = {
    if (options.reliabilityLevel == SQLServerBulkJdbcOptions.BEST_EFFORT) {
      SingleInstanceWriteStrategies.write(df, colMetaData, options, appId)
    }
    else if(options.reliabilityLevel == SQLServerBulkJdbcOptions.NO_DUPLICATES) {
      ReliableSingleInstanceStrategy.write(df, colMetaData, options, appId)
    } else {
      throw new SQLException(
        s"""Invalid value for reliabilityLevel """)
    }
  }

  override def createTable(conn: Connection, df: DataFrame, options: SQLServerBulkJdbcOptions): Unit = {
    mssqlCreateTable(conn, df, options)
  }

  override def dropTable(conn: Connection, dbtable: String, options: JDBCOptions): Unit = {
    JdbcUtils.dropTable(conn,dbtable, options )
  }
}
