package com.microsoft.sqlserver.jdbc.spark

import java.sql.{ResultSetMetaData, SQLException}

import com.microsoft.sqlserver.jdbc.spark.BulkCopyUtils.{getDBNameFromURL, savePartition}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Implements the BEST_EFFORT write strategy for Data Pools. All executors insert
 * into a user specified table directly. Write to table is not transactional and
 * may results in duplicates in executor restart scenarios.
 */
object BestEffortDataPoolStrategy extends DataIOStrategy with Logging {
  /**
   * write finds the datapool host names, maps the hostnames to respective exectuors,
   * creates connection urls and delegates control to the executors to start the writing process.
    * The Design of interacting with the data pools proceeds as follows
    * 1. Use Sql Server Master instance to create the data source and external table.
    * Note , per data pool design external tables cannot be created in the 'master' database and external table
    * creation is data base altering and that cannot be done. So the external table must be created in
    * user specified database only.
    * 2. Find the data pool nodes. This uses a utility getDataPoolNodeList
    * 3. Use the returned node hostnames to construct URL and create connections to each of the
    * data nodes. Pass this connection to each of the executors to write their partition
    * of the dataframe.
    */
  def write(
        df: DataFrame,
        colMetaData: Array[ColumnMetadata],
        options: SQLServerBulkJdbcOptions,
        appId: String): Unit = {
    logInfo("write : best effort  write to datapools called")
    //val dfColMetadata: Array[ColumnMetadata] = getColMetadataMap(metadata)
    val dfColMetadata = colMetaData
    val hostnames = DataPoolUtils.getDataPoolNodeList(options)
    if(hostnames.isEmpty) {
      throw new SQLException(
        s""" ${hostnames.length} datapool nodes found.
           |DataPools are not configured or non reachable:""".stripMargin)
    }
    logDebug(s"write:${hostnames.length} datapool nodes found : ${hostnames.mkString(" ")}")
    logDebug("write:Will order the Executor action now")
    val dbname = getDBNameFromURL(options.url)
    logDebug("write:user URL " + s"$dbname")
    val results = df.rdd.mapPartitionsWithIndex(
      (index, iterator) => {
         options.dataPoolDistPolicy match {
          case "ROUND_ROBIN" => {
            val hostname = hostnames(index % hostnames.length)
            logInfo(s"write: partition index $index to host $hostname " +
              s"with distribution policy ROUND_ROBIN")
            saveToDataPoolNode(iterator,hostname,options, dfColMetadata)
          }
          case "REPLICATED" => {
            val host_itr_map = getHostIteratorMap(iterator, hostnames)
            host_itr_map.foreach( {case (hostname, itr) =>
              logInfo(s"write: partition index $index which hasElem is ${itr.hasNext} " +
                s"to host $hostname with distribution policy REPLICATED")
              saveToDataPoolNode(itr,hostname,options, dfColMetadata)
            })
         }
          case _ => {
            throw new SQLException(
              s""" Invalid value in dataPoolDistPolicy ${options.dataPoolDistPolicy}  .
                 | Internal feature usage error:""".stripMargin)
          }
        }
        logInfo(s"write:Executor: Saved partition index $index")
        Iterator[Int](0)
      }
    ).collect()
  }

  /**
   * saveToDataPoolNode create the right data pool connection url
   * and calls saves parition to save the rows to the data pool node.
   */
  def saveToDataPoolNode(
        iterator: Iterator[Row],
        hostname:String,
        options:SQLServerBulkJdbcOptions,
        dfColMetadata:Array[ColumnMetadata]) : Unit = {
    logInfo(s"write: to hostname $hostname")
    val url = DataPoolUtils.createDataPoolURL(hostname, options)
    val newOptions = new SQLServerBulkJdbcOptions(options.parameters + ("url" -> url))
    savePartition(iterator, options.dbtable, dfColMetadata, newOptions)
  }

  /**
   * Used in REPLICATE scenario where a single partition represented by an iterator is written
   * to multiple connection. Scala iterators can only be traversed once. As a result need
   * clone the orignal iterator per hostname.
   * getHostIteratorMap returns a map of hostnames and associated iterator[Row].
   * This iterator can then be used for writing to the respective host.
   * Refer : https://www.scala-lang.org/api/2.11.2/index.html#scala.collection.Iterator
   */
  def getHostIteratorMap(
      itr:Iterator[Row],
      hostnames:List[String]) : Map[String,Iterator[Row]] ={
    var host_itr_map =  scala.collection.mutable.Map[String,Iterator[Row]]()
    var itr_orignal = itr
    hostnames.foreach(hostname => {
      val (itr_new,itr_second) = itr_orignal.duplicate
      host_itr_map(hostname) = itr_new
      itr_orignal = itr_second
    })
    host_itr_map.toMap
  }
}
