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
      val t1 = System.currentTimeMillis()                                   
      //val dfColMetadata: Array[ColumnMetadata] = getColMetadataMap(metadata)
      val dfColMetadata = colMetaData
      df.rdd.foreachPartition(iterator =>
       {    
          val t1 = System.currentTimeMillis()                                   
          savePartition(iterator, options.dbtable, dfColMetadata, options)
          logInfo(s"write : Time to savePartition ${System.currentTimeMillis() - t1} ")
          println(s"write : Time to savePartition ${System.currentTimeMillis() - t1} ")
      })
      logInfo(s"write : Time to write all ${System.currentTimeMillis() - t1} ")
      println(s"write : Time to write all ${System.currentTimeMillis() - t1} ")         
    }
}