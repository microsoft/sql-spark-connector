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
import java.util.Properties
import java.sql.{Connection, Statement, DriverManager, Date, Timestamp}
import org.apache.spark.sql.{SparkSession, SaveMode, Row, DataFrame}
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types._

class DataPoolTests(testUtils:Connector_TestUtils) {
    val testTableSuffix = "dptest"
    val testType = testUtils.getTestType()
    val spark = testUtils.getSparkSession()
    val log = testUtils.createModuleLogger("DataPoolTests")
    
    def runner() {
        //find all method that start with "test_" and invoke them
        val fqTestPrefix = this.getClass.getSimpleName + s".${testUtils.testCasePrefix}_"
        for(method<- this.getClass.getMethods) {
            if(method.toString.contains(fqTestPrefix)){
                log.info(s"Starting DataPool test :  ${method.toString()}")
                method.invoke(this)
                log.info(s"Ending DataPool test :  ${method.toString()}")
            } 
        }      
    } 

    /*
     * The following section has data pool test cases. The test remain the same as those for master instance.
     * The only diffirence is that data pools ( rather than master instance) are used to insert. Datapools 
     * only support round-robin.
     * TODO : Add more test cases. Repeate all test cases for data pools.
     */

   /*
    * test_gci_data_pool_read_write
    * The test demonstrates write (overwrite, append) and read of external table from data pool
    * Steps
    *   1. Write data to data pool external table with Overwrite mode followed by Append mode.
    *   2. Reads that table using data pools ( by specifing the datasource while reading)
    * The test case knowingly specifies the data source to ensure that specifing the
    * data source does not result in a problem at a JDBC level. Customer may not specify
    * data source when reading and this should *not* results in an error
    * Note
    *      1. Data pool instances do not support reading of tables.
    *      2. Connector's read logic defaults to spark generic JDBC connector.
    * On the read path the test basically check if the generic Spark JDBC connector can read
    * external tables that were created using data pools while the data source
    * is still specified in option.
    *
   */
    def test_gci_dp_read_write() {
        val table_name = s"test_data_pool_write1"
        val df = testUtils.create_toy_df()
        testUtils.df_write(df, SaveMode.Overwrite, table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource)

        // Insert into the external tables ( Append mode)
        testUtils.df_write(df, SaveMode.Append, table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        val new_df = testUtils.df_read(table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        assert(2*df.count == new_df.count)
        testUtils.drop_external_table(table_name)
    }

    /*
     * test_gci_read_external_tables
     * The test demonstrates write to data pool external table and then read from master instance.
     * The test creates an external table via connector's data pool feature , but reads it via master instance.
     * Note 
     *      1. Data pool instances do not support reading of tables. 
     *      2. Connector's read logic defaults to spark generic JDBC connector.
     * So this test basically check if the generic Spark JDBC connector can read 
     * external tables that were created using data pools.
    */
    def test_gci_dp_read_external_tables() {
        //if (dataSrc != "bulk") return
        val table_name = s"test_datapool_table"
        val ori_df = testUtils.create_toy_df()

        // Insert into the external tables.
        testUtils.df_write(ori_df, SaveMode.Overwrite, table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource)

        //Read back from the resultant external table that's created
        val new_df = testUtils.df_read(table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        assert(ori_df.count() == new_df.count())
        log.info(s"orignal DF ${ori_df.show(5)}")
        log.info(s"new DF ${new_df.show(5)}")
        testUtils.drop_external_table(table_name)
    }

    /*
     * test_ci_dp_ext_table_with_spark_types
     * test_data_pool_truncate
     * Test external table overwrite with truncation
     * Check that trucate can be used with overwrite
     *
     */
    def test_gci_dp_truncate_success() : Unit = {
        val table = "test_gci_dp_truncate_success"
        val dataSrc = "spark_mssql_gci_datasrc"
        val oriDf = testUtils.create_toy_df()
        testUtils.df_write(oriDf, SaveMode.Overwrite, table, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        var resDf = testUtils.df_read(table, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        assert(oriDf.count() == resDf.count())
        log.info(s"table written \n")
        testUtils.df_write(oriDf, SaveMode.Overwrite, table, isTruncate="true", dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        log.info(s"truncate with df succeeded \n")
        resDf =  testUtils.df_read(table, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        assert(oriDf.count() == resDf.count())
        testUtils.drop_external_table(table)
    }

    /*
     * test_data_pool_truncate
     * Test external table overwrite with truncation
     * Check that trucate done with a df with a diffirent schema raises an exception
     *
     */
    def test_gci_dp_truncate_error() : Unit = {
        val table = "test_gci_dp_truncate_error"
        val dataSrc = "test_datapool_table_source"
        val oriDf = testUtils.create_toy_df()
        testUtils.df_write(oriDf, SaveMode.Overwrite, table, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        var resDf = testUtils.df_read(table, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        assert(oriDf.count() == resDf.count())
        log.info(s"table written \n")
        val schema = StructType (Seq (
            StructField("itemNum", IntegerType, true),
            StructField("entry_word", StringType, true)
        ))
        val data = Seq(
            Row(1,"hello"),
            Row(2, "hellos")
        )
        log.info(s"new DF prepared. Calling overwrite(trucate). Expecting an exception \n")
        val newDf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        var exceptRaised = false
        try {
            testUtils.df_write(newDf, SaveMode.Overwrite, table, isTruncate="true", dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        }
        catch {
            case ex:Exception =>
                log.info(s"Exception raised ${ex.getMessage()} \n")
                exceptRaised = true
        }
        assert(exceptRaised==true)
        testUtils.drop_external_table(table)
    }

    /*
     * test_gci_dp_threePartName_owar
     * OverWrite/Append and Read (OWAR) to External tables using 3 part names
     */
    def test_gci_dp_threePartName_owar() {
        val threePartName = testUtils.createThreePartName("test_dp_threePartName_owar")
        log.info(s"Tablename is $threePartName \n")
        val df = testUtils.create_toy_df()
        log.info("Operation Overwrite, append and read\n")
        testUtils.df_write(df, SaveMode.Overwrite, threePartName, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        testUtils.df_write(df, SaveMode.Append, threePartName, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        val df_result = testUtils.df_read(threePartName, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        assert(df_result.count() == 2*df.count())
        testUtils.drop_external_table(threePartName)
    }

    /*
     * test_gci_dp_twoPartName_owar
     * OverWrite/Append and Read (OWAR) to External tables using 2 part names
     */
    def test_gci_dp_twoPartName_owar() {
        val twoPartName = testUtils.createTwoPartName("test_dp_twoPartName_owar")
        log.info(s"Tablename is $twoPartName \n")
        log.info(s"Tablename is $twoPartName \n")
        val df = testUtils.create_toy_df()
        log.info("Operation Overwrite, append and read\n")
        testUtils.df_write(df, SaveMode.Overwrite, twoPartName, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        testUtils.df_write(df, SaveMode.Append, twoPartName, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        val df_result = testUtils.df_read(twoPartName, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
        assert(df_result.count() == 2*df.count())
        testUtils.drop_external_table(twoPartName)
    }

  def test_gci_dp_secureURL_write() {
    val table_name = s"test_gci_dp_secureURL_write${testType}"
    val df = testUtils.create_toy_df()
    testUtils.df_write(df, SaveMode.Overwrite, table_name,
                        dataPoolDataSource = testUtils.dataPoolExternalDataSource,
                        encrypt = "false", trustServerCertificate = "true")
    testUtils.df_write(df, SaveMode.Append, table_name,
                        dataPoolDataSource = testUtils.dataPoolExternalDataSource,
                        encrypt = "true", trustServerCertificate = "true")

    var result = testUtils.df_read(table_name)
    assert(df.schema == result.schema)
    assert(result.count == 2*df.count())
    log.info("test_gci_dp_secureURL_write : Exit")
    testUtils.drop_external_table(table_name)
  }

  def test_gci_dp_replicated_tables() {
    var total_rows_expected = 0L
    val table_name = s"test_gci_dp_replicated_tables${testType}"
    val df = testUtils.create_toy_df()
    testUtils.df_write(df, SaveMode.Overwrite, table_name,
                        dataPoolDataSource = testUtils.dataPoolExternalDataSource,
                        encrypt = "false", trustServerCertificate = "true",
                        datapoolDistPolicy="REPLICATED")

    testUtils.df_write(df, SaveMode.Overwrite, table_name,
      isTruncate = "true", dataPoolDataSource = testUtils.dataPoolExternalDataSource,
      encrypt = "false", trustServerCertificate = "true",
      datapoolDistPolicy="REPLICATED")

    total_rows_expected = df.count()
    // Check exception is raised if overwrite is done with a diffirent policy.
    var exception_raised = false
    try {
      testUtils.df_write(df, SaveMode.Overwrite, table_name,
        isTruncate = "true", dataPoolDataSource = testUtils.dataPoolExternalDataSource,
        encrypt = "false", trustServerCertificate = "true",
        datapoolDistPolicy="ROUND_ROBIN")
    }
    catch {
      case e: Throwable => {
        exception_raised = true
      }
    }
    assert(exception_raised==true)

    // Append as REPLICATED TABLE and check count from master.
    testUtils.df_write(df, SaveMode.Append, table_name,
                        dataPoolDataSource = testUtils.dataPoolExternalDataSource,
                        encrypt = "true", trustServerCertificate = "true",
                        datapoolDistPolicy="REPLICATED")
    total_rows_expected = 2*df.count
    var result = testUtils.df_read(table_name)
    assert(df.schema == result.schema)
    assert(result.count == total_rows_expected)

    // Check that each data pool node has the full table
    val dp_nodes = testUtils.getDataPoolNodes()
    val dp_node_conn = testUtils.dataPoolConnectionMap()
    dp_nodes.foreach(dp_host => {
      val stmt = dp_node_conn(dp_host).createStatement()
      val rs = stmt.executeQuery(s"select count(*) as total from $table_name")
      while(rs.next()) {
        val count = rs.getInt("total")
        log.info(s"test_gci_dp_replicated_tables : from dpnode $dp_host " +
          s"retrieved $count rows from table $table_name")
        assert(count == total_rows_expected)
      }
    })

    log.info("test_gci_dp_replicated_tables : Exit")
    testUtils.drop_external_table(table_name)
  }

  def test_gci_reordered_columns() {
    val table_name = s"test_gci_reordered_columns${testType}"
    log.info("test_gci_reordered_columns : Created DF")

    // Create a dataframe with default column order i.e. 0,1,2,3,4
    val df = testUtils.dfTableUtility.create_df()

    // Create table with diffirent column order than  dataframe
    testUtils.dfTableUtility.create_external_table(table_name,List(1,0,4,3,2), testUtils.dataPoolExternalDataSource)
    log.info("test_gci_reordered_columns : Created table")
    testUtils.df_write(df, SaveMode.Append, table_name,
                        dataPoolDataSource = testUtils.dataPoolExternalDataSource)

    log.info("test_gci_reordered_columns : Append succcessful")
    var result = testUtils.df_read(table_name)

    assert(df.count() == result.count())
    testUtils.dfTableUtility.check_ret_values(result)
    log.info("test_gci_reordered_columns : Read back table and checked values returned")

    val cols = testUtils.dfTableUtility.getDfCols(List(4,3,2,1,0))

    var df_reordered = df.select(cols.head, cols.tail: _*)
    testUtils.df_write(df_reordered, SaveMode.Overwrite, table_name,
                        dataPoolDataSource = testUtils.dataPoolExternalDataSource,
                        isTruncate = "true")
    result = testUtils.df_read(table_name)
    assert(df_reordered.count() == result.count())
    log.info("test_gci_reordered_columns : Reordered Write overwrite with truncate")

    testUtils.df_write(df_reordered, SaveMode.Append, table_name,
      dataPoolDataSource = testUtils.dataPoolExternalDataSource)
    result = testUtils.df_read(table_name)
    assert(2*df_reordered.count() == result.count())
    log.info("test_gci_reordered_columns : Reordered write append")

    // The step below finally overwrites the table in same order as data frame
    testUtils.df_write(df_reordered, SaveMode.Overwrite, table_name,
      dataPoolDataSource = testUtils.dataPoolExternalDataSource)
    result = testUtils.df_read(table_name)
    assert(df_reordered.count() == result.count())
    log.info("test_gci_reordered_columns : Reordered Write overwrite without truncate")
    testUtils.drop_external_table(table_name)
  }

    /*
     * test_ci_dp_ext_table_with_spark_types
     * The test checks creation of data pool external tables from Spark SQL connector using all Spark datatypes.
     * Possible SQL Server data type in TypeConversionToTest.scala
     * NOTE on varchar(max) as type in MSSQL external tables. 
     * Large strings is a common big data scenario. Such tables will result in a table with 
     * a column type as varchar(max). Currently creating an external table over 
     * HDFS mapped to a varchar(max) is not supported. The only way to support that scenario is 
     * 1. read table into spark and 
     * 2. Create equivalent sql external table via mssqlspark connector. 
     */
     def test_ci_dp_ext_table_with_spark_types() {
        val list_of_types = TypeCoversionToTest.spark_mssql_type_list_actual
        for (i <- 0 to list_of_types.length-1) {
            val combo = list_of_types(i)
            val table_name =  s"test_data_types_${i}_${testType}_${testTableSuffix}"
            log.info(s"datype ${combo}")

            val schema = StructType(Seq(StructField("col1", combo._1, true)))
            val data = Seq(Row(combo._4))
            val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
            testUtils.df_write(df, SaveMode.Overwrite, table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
            testUtils.drop_external_table(table_name)
        }
    }

    /*
     * test_ci_dp_read_write_to_existing_ext_table
     * the test checks that MSSQLSpark connector can write and read to existing external tables.
     * The test first creates an external table of a given type and then uses MSSQLSparkconnector to
     * write and read from the table as a external data pool table.
     */
    def test_ci_dp_read_write_to_existing_ext_table() {
        val mssql_types = TypeCoversionToTest.mssql_to_spark_type_create_ex_table
        for (i <- 0 to mssql_types.length-1) {
            val combo = mssql_types(i)
            val table_name = s"test_existing_ex_table${i}_${testType }_${testTableSuffix}"
            val query_table = testUtils.get_create_ex_table_query(table_name, testUtils.dataPoolExternalDataSource, combo._1, combo._2)
            try {
                testUtils.executeQuery(query_table)
                assert(testUtils.df_read(table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource).count == 1)
                val schema = StructType(Seq(StructField("col1", combo._3, true)))
                val data = Seq(Row(combo._4))
                val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
                testUtils.df_write(df, SaveMode.Append, table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource)
                assert(testUtils.df_read(table_name, dataPoolDataSource = testUtils.dataPoolExternalDataSource).count == 2)
                testUtils.drop_external_table(table_name)
            } catch {
                 case e: Throwable => {
                    log.info(s"Failed for pair datatype/value ${combo} with Error ${e.getMessage()} ")
                    throw e
                }
            }
        }
    }
}
