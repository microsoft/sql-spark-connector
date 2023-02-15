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

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/*
 * MasterInstanceTest
 * test cases for master instance. Most test can we used for Spark JDBC 
 * and MSSQLSparkConntector. 
 * @utils : Take Connector_Utils as parameter.getTestType() is used to determin 
 * if test is for MSSQLSparkConnector or JDBC connector.
 */
class MasterInstanceTest(testUtils:Connector_TestUtils) {
    val testType = testUtils.getTestType()
    val spark = testUtils.getSparkSession()
    val log = testUtils.createModuleLogger("MasterInstanceTest")

    def runner() {
        //find all method that start with prefix as specified by testUtils.testType and invoke them
        val fqTestPrefix = this.getClass.getSimpleName + s".${testUtils.testCasePrefix}_"
        for(method<- this.getClass.getMethods) {
            if(method.toString.contains(fqTestPrefix)){
                log.info(s"Starting MasterInstance tests:" + method.toString() + " ReliablityMode " + testUtils.runWithReliableConnector)
                method.invoke(this)
                log.info(s"Ending MasterInstance tests : " +  method.toString() + " ReliablityMode " + testUtils.runWithReliableConnector)
            }
        }
    }

    // Test creation of an empty dataframe
    def test_gci_empty_dataframe() {
        val table_name = s"test_empty_dataframe_${testType}"

        val schema = StructType(Seq(
            StructField("entry", IntegerType, true)
        ))
        val empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        testUtils.df_write(empty_df, SaveMode.Overwrite, table_name)
        var result = testUtils.df_read("information_schema.tables")
        assert(result.where(s"TABLE_NAME = '${table_name}'").count == 1)
        result = testUtils.df_read(table_name)
        assert(result.count == 0)
        testUtils.drop_test_table(table_name)
    }

    // Test basic functionalities of reading and writing to database
    def test_gci_read_write() {
        val table_name = s"test_basic_read_write_${testType }"
        val df = testUtils.create_toy_df()
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        var result = testUtils.df_read(table_name)
        assert(testUtils.compareSchemaIgnoreColsMetadata(df.schema, result.schema))
        var query = s"(select * from ${table_name} where entry_number > 100) emp_alias"
        result = testUtils.df_read(query)
        assert(result.count == 2)
        query = s"(select * from ${table_name} where len(entry_word) = 5) emp_alias"
        result = testUtils.df_read(query)
        assert(result.count == 4)
        log.info("test_basic_read_write : Exit")
        testUtils.drop_test_table(table_name)
    }

    // Test writing rows with null values
    def test_gci_null_values() {
        val table_name = s"test_null_values_${testType }"

        val schema = StructType(Seq(
            StructField("entry_number", IntegerType, true),
            StructField("entry_word", StringType, true)
        ))
        val data = Seq(
            Row(24, "hello"),
            Row(null, "world"),
            Row(99, null),
            Row(null, null),
            Row(149, "monitor")
        )
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        var result = testUtils.df_read(table_name)
        assert(df.count == result.count)
        val df_rows = df.orderBy("entry_number", "entry_word").collect()
        val result_rows = result.orderBy("entry_number", "entry_word").collect()
        for (i <- 0 to df.count.toInt - 1) {
            assert(df_rows(i)(0) == result_rows(i)(0))
            assert(df_rows(i)(1) == result_rows(i)(1))
        }
        testUtils.drop_test_table(table_name)
    }

    // Test appending rows with correct and incorrect schemas to table in database
    def test_gci_append_rows() {
        val table_name = s"test_append_rows_${testType}"
        val df = testUtils.create_toy_df()
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        var result = testUtils.df_read(table_name)
        assert(result.count == 5)
        // Append row with correct schema
        var data = Seq(Row(151, "mew"))
        var schema = StructType(Seq(
            StructField("entry_number", IntegerType, true),
            StructField("entry_word", StringType, true)
        ))
        var extra_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        testUtils.df_write(extra_df, SaveMode.Append, table_name)
        result = testUtils.df_read(table_name)
        assert(result.count == 6)
        // Append row with incorrect schema
        var error_caught = false
        data = Seq(Row(151, "mew", 22))
        schema = StructType(Seq(
            StructField("entry_number", IntegerType, true),
            StructField("entry_word", StringType, true),
            StructField("extra", IntegerType, true)
        ))
        extra_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        try {
            testUtils.df_write(extra_df, SaveMode.Append, table_name)
        } catch {
            case e: Exception => {
                error_caught = true
            }
        }
        result = testUtils.df_read(table_name)
        assert(result.count == 6)
        assert(error_caught)
        testUtils.drop_test_table(table_name)
    }

    // Test overwriting a table via truncation with correct and incorrect schema
    def test_gci_truncate_table() {
        val table_name = s"test_truncate_table_${testType }"
        var df = testUtils.create_toy_df()
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        var result = testUtils.df_read(table_name)
        assert(result.count == df.count)
        // Overwrite existing table via truncation
        var data = Seq(Row(151, "mew"), Row(25, "pikachu"))
        var schema = StructType(Seq(
            StructField("entry_number", IntegerType, true),
            StructField("entry_word", StringType, true)
        ))
        df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        testUtils.df_write(df, SaveMode.Overwrite, table_name, isTruncate="true")
        result = testUtils.df_read(table_name)
        assert(result.count == df.count)
        // Try to truncate and add dataframe with incorrect schema
        var error_caught = false
        data = Seq(Row(150, "mewtwo", 22))
        schema = StructType(Seq(
            StructField("entry_number", IntegerType, true),
            StructField("entry_word", StringType, true),
            StructField("extra", IntegerType, true)
        ))
        val wrong_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        try {
            testUtils.df_write(wrong_df, SaveMode.Overwrite, table_name, isTruncate="true")
        } catch {
            case e: Exception => {
                error_caught = true
            }
        }
        assert(error_caught)
        testUtils.drop_test_table(table_name)
    }

    // Test case sensitivity of column names
    def test_gci_case_sensitivity() {
        val table_name = s"test_case_sensitivity_${testType }"
        val df = testUtils.create_toy_df()
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        var result = testUtils.df_read(table_name)
        assert(result.count == 5)
        // Append row with a column name that has different casing
        spark.sqlContext.setConf("spark.sql.caseSensitive", "true")
        var error_caught = false
        var data = Seq(Row(151, "mew", 22))
        var schema = StructType(Seq(
            StructField("entry_NumbeR", IntegerType, true),
            StructField("entry_word", StringType, true)
        ))
        var extra_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        try {
            testUtils.df_write(extra_df, SaveMode.Append, table_name)
        } catch {
            case e: Exception => {
                error_caught = true
            }
        }
        result = testUtils.df_read(table_name)
        assert(result.count == 5)
        assert(error_caught)
        // Append row with a column name that has different casing
        spark.sqlContext.setConf("spark.sql.caseSensitive", "false")
        testUtils.df_write(extra_df, SaveMode.Append, table_name)
        result = testUtils.df_read(table_name)
        assert(result.count == 6)
        testUtils.drop_test_table(table_name)
    }

    // Test precision and scale values of data types
    def test_gci_precision_scale() {
        val table_name = s"test_precision_scale_${testType }"
        val schema = StructType(Seq(
            StructField("entry_number", DecimalType(5, 2), true)
        ))
        val data = Seq(
            Row(BigDecimal(123.45)),
            Row(BigDecimal(678.90)),
            Row(BigDecimal(135.79))
        )
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        var result = testUtils.df_read(table_name)
        assert(testUtils.compareSchemaIgnoreColsMetadata(df.schema, result.schema))
        assert(df.count == result.count)
        val df_rows = df.orderBy(asc("entry_number")).collect()
        val result_rows = result.orderBy(asc("entry_number")).collect()
        for (i <- 0 to df.count.toInt - 1) {
            assert(df_rows(i)(0) == result_rows(i)(0))
        }
        testUtils.drop_test_table(table_name)
    }

    // Test SQL Server isolation levels settings.
    // This test applies only to our custom MSSQL connector
    // Note that this test does not test the isolation level functionality. It just test that setting does not
    // result in unexpected exceptions.

    def test_gci_isolation_level() {
        val table_name = s"test_isolation_level_${testType }"

        if (testType  == "bulk") {
            val df = testUtils.create_toy_df()
            testUtils.df_write(df, SaveMode.Overwrite, table_name, isoLevel="READ_UNCOMMITTED")
            var result = testUtils.df_read(table_name)
            assert(df.count == result.count)
            log.info("test_isolation_level : READ_UNCOMMITTED succeded")
            testUtils.df_write(df, SaveMode.Overwrite, table_name, isoLevel="READ_COMMITTED")
            result = testUtils.df_read(table_name)
            assert(df.count == result.count)
            log.info("test_isolation_level : READ_COMMITTED succeded")
            testUtils.df_write(df, SaveMode.Overwrite, table_name, isoLevel="REPEATABLE_READ")
            result = testUtils.df_read(table_name)
            assert(df.count == result.count)
            log.info("test_isolation_level : REPEATABLE_READ succeded")
            //SQL supports snapshot isolution, but needs to be explicitly enabled.
            var snapshot_isolation_enable = false
            var error_caught = false
            try{
                log.info(s"test_isolation_level : SNAPShort write start")
                testUtils.df_write(df, SaveMode.Overwrite, table_name, isoLevel="SNAPSHOT")
                log.info(s"test_isolation_level : SNAPShort write done")
                result = testUtils.df_read(table_name)
                log.info(s"test_isolation_level : SNAPShort read done")
                log.info(s"test_isolation_level : SNAPShort ${df.count} ${result.count}")
                log.info(s"Assert counts")
                assert(df.count == result.count) 
            } catch {
               case e : Exception => {
                   log.info(s"test_isolation_level : check if snapshot_isolation_enable == false ")
                   if(snapshot_isolation_enable == false) {
                       log.info(s"test_isolation_level : snapshot_isolation_enable == false ")
                       error_caught = false
                   }
                   else {
                       log.info(s"test_isolation_level : snapshot_isolation_enable == true ")
                       error_caught = true
                   }
               }
           }
           assert(error_caught == false)
           log.info("test_isolation_level : SNAPSHOT succeded")
           error_caught = false
           try {
               log.info("test_isolation_level : isoLevel = NONE")
               testUtils.df_write(df, SaveMode.Overwrite, table_name, isoLevel="NONE")
           } catch {
               case e: Exception => {
                   log.info("test_isolation_level : isoLevel = NONE Exception")
                   error_caught = true
               }
           }
           assert(error_caught)
           log.info("test_isolation_level : all done")
        }
        testUtils.drop_test_table(table_name)
    }

//    // TODO: Activate this test when Aris enables MSDTC
//    // Test distributed transaction feature for assuring no duplicates copied
//    def idem_test_basic() {
//        val table_name = "test_basic_write_no_duplicates_bulk"
//         try {
//             val df = testUtils.create_toy_df()
//             testUtils.df_write(df, SaveMode.Overwrite, table_name,
//                     reliabilityLevel="NO_DUPLICATES")
//
//             val result = df_read(table_name)
//             val df_rows = df.orderBy(asc("entry_number")).collect()
//             val result_rows = result.orderBy(asc("entry_number")).collect()
//             for (i <- 0 to df.count.toInt - 1) {
//                 assert(df_rows(i)(0) == result_rows(i)(0))
//             }
//         } finally {
//             testUtils.drop_test_table(table_name)
//         }
//    }

//    // Must be called in isolation. This function will crash the current Spark
//    // application with error code -1.
//    def idem_test_crash1() {
//        val table_name = "test_data_idempotency_bulk"
//        testUtils.drop_test_table(table_name)
//        val df = testUtils.create_toy_df()
//        spark.sqlContext.setConf("spark.task.maxFailures", "1")
//        testUtils.df_write(df, SaveMode.Overwrite, table_name,
//                reliabilityLevel="NO_DUPLICATES", testDataIdempotency="true")
//    }

//    // Must be called in isolation. This function will check to make sure that
//    // the table copied in "test1_data_idempotency_crash()" did not persist
//    def idem_test_crash2() {
//        val table_name = "test_data_idempotency_bulk"
//        try {
//            val result = testUtils.df_read(table_name)
//            assert(result.count == 0)
//        } finally {
//            testUtils.drop_test_table(table_name)
//        }
//    }

    // Test several permutations of read queries.    
    def test_gci_limit_escape() {
        val table_name_1 = s"test_limit_escape_t1_${testType }"
        val table_name_2 = s"test_limit_escape_t2_${testType }"
        val table_name_3 = s"test_limit_escape_t3_${testType }"
        val table_name_4 = s"test_limit_escape_t4_${testType }"
        val table_name_vec = Vector(table_name_1, table_name_2, table_name_3, table_name_4)
        val insert_query_1 = s"insert into ${table_name_1} values (1, 1, 'col3', 'col4'), (2, 2, 'row2 '' with '' quote', 'row2 with limit {limit 22} {limit ?}'), (3, 3, 'row3 with subquery (select * from t1)', 'row3 with subquery (select * from (select * from t1) {limit 4})'), (4, 4, 'select * from t1 {limit 4} ''quotes'' (braces)', 'ucase(scalar function)'), (5, 5, 'openquery(''server'', ''query'')', 'openrowset(''server'',''connection string'',''query'')')"
        val insert_query_2 = s"insert into ${table_name_2} values (11, 11, 'col33', 'col44')"
        val insert_query_3 = s"insert into ${table_name_3} values (111, 111, 'col333', 'col444')"
        val insert_query_4 = s"insert into ${table_name_4} values (1111, 1111, 'col4444', 'col4444')"
        val insert_query_vec = Vector(insert_query_1, insert_query_2, insert_query_3, insert_query_4)

        // Create tables and apply "insert" queries
        for (i <- 0 to table_name_vec.length-1) {
            val table_create_query = s"""drop table if exists ${table_name_vec(i)}; create table ${table_name_vec(i)} 
                (col1 int, col2 int, col3 varchar(100), col4 varchar(100), id int identity(1,1) primary key);"""
            testUtils.executeQuery(table_create_query + insert_query_vec(i))
        }
        // Query 1: Test simple "top" query without limit syntax
        var query = s"(select TOP 1 * from ${table_name_vec(0)}) emp_alias"
        var result = testUtils.df_read(query)
        assert(result.count == 1)
        assert(result.head.getInt(4) == 1)
        // Query 2: Test parentheses in limit syntax
        query = s"(select * from ${table_name_vec(0)} {limit ( (  (2)))}) emp_alias"
        result = testUtils.df_read(query)
        assert(result.count == 2)
        assert(result.orderBy(asc("id")).take(2)(0).getInt(4) == 1)
        assert(result.orderBy(asc("id")).take(2)(1).getInt(4) == 2)
        // Query 3: Test offset syntax in string literals
        query = s"(select * from ${table_name_vec(0)} where col3 = '{limit 1 offset 2}') emp_alias"
        result = testUtils.df_read(query)
        assert(result.count == 0)
        // Query 4: Test limit syntax with arbitary spaces in multiple levels
        //          of nested subqueries
        query = s"""(select j1.id from (select * from (select * from ${table_name_vec(0)} {limit 3}) j3 {limit 2}) 
            j1 join (select * from ${table_name_vec(1)} {limit 4}) j2 on j1.id = j2.id {limit      1}) emp_alias"""
        result = testUtils.df_read(query)
        assert(result.count == 1)
        assert(result.head.length == 1)
        assert(result.head.getInt(0) == 1)
        // Query 5: Test more complex query with multiple levels of nested
        //          subqueries and limit syntax
        query = s"""(select j1.id from ( ((select * from (select * from ${table_name_vec(0)} {limit 3}) 
            j3 {limit 2}))) j1 join (select j4.id from ((((select * from ${table_name_vec(2)} {limit 5})))) j4 join 
            (select * from  ${table_name_vec(3)} {limit 6}) j5 on j4.id = j5.id ) j2 on j1.id = j2.id {limit 1}) emp_alias"""
        result = testUtils.df_read(query)
        assert(result.count == 1)
        assert(result.head.length == 1)
        assert(result.head.getInt(0) == 1)

        for (table_name <- table_name_vec) {
            testUtils.drop_test_table(table_name)
        }
    }

    /*
     * OverWrite/Append and Read (OWAR) to SQL tables using 3 part names     *
     */
    def test_gci_threePartName_owar() {
        val table = s"test_gci_threePartName_owar"
        val threePartName = testUtils.createThreePartName(table)
        log.info(s"Tablename is $threePartName \n")
        val df = testUtils.create_toy_df()
        log.info("Operation Overwrite, append and read\n")
        testUtils.df_write(df, SaveMode.Overwrite, threePartName)
        testUtils.df_write(df, SaveMode.Append, threePartName)
        val df_result = testUtils.df_read(threePartName)
        assert(df_result.count() == 2*df.count())
        testUtils.drop_test_table(threePartName)
    }

    /*
     * OverWrite/Append and Read (OWAR) to SQL tables using 2 part names     *
     */
    def test_gci_twoPartName_owar() {
        val table = s"test_gci_twoPartName_owar"
        val twoPartName = testUtils.createTwoPartName(table)
        log.info(s"Tablename is $twoPartName \n")
        val df = testUtils.create_toy_df()
        log.info("Operation Overwrite, append and read\n")
        testUtils.df_write(df, SaveMode.Overwrite, twoPartName)
        testUtils.df_write(df, SaveMode.Append, twoPartName)
        val df_result = testUtils.df_read(twoPartName)
        assert(df_result.count() == 2*df.count())
        testUtils.drop_test_table(twoPartName)
    }

    /*
     * OverWrite/Append and Read (OWAR) to SQL tables using 1 part name within square brackets     *
     */
    def test_gci_tbNameInBracket_owar() {
        val table_name = s"[test_gci_tbNameInBracket_owar]"
        log.info(s"Table name is $table_name \n")
        val df = testUtils.create_toy_df()
        log.info("Operation Overwrite, append and read\n")
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        testUtils.df_write(df, SaveMode.Append, table_name)
        var result = testUtils.df_read(table_name)
        assert(result.count() == 2 * df.count())
        log.info("test_gci_tbNameInBracket_owar : Exit")
        testUtils.drop_test_table(table_name)
    }

    /*
    * The test checks that all supported datatype can be used to create tables
    * Possible SQL Server data type are described at the link below
    * https://docs.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types?view=sql-server-2017
    *
    * NOTE : varchar(max) added as a explicit test cases here.
    * Large strings is a common big data scenario. Such tables will result in a table with a column type as varchar(max)
    * Currently creating an external table over HDFS mapped to a varchar(max) is not supported. The only way to support that scenario
    * is 1. read table into spark and 2. Create equivalent sql table via jdbc or mssqlspark connector.
    *
    */
    def test_ci_data_types() {
        val combos = TypeCoversionToTest.mssql_to_spark_type_create_table
        for (i <- 0 to combos.length-1) {
            val combo = combos(i)
            val table_name = s"test_data_types_${i}_${testType}"
            val query = testUtils.get_create_query(table_name, combo._1, combo._2)
            log.debug(s"Create table query  ${query}")
            try {
                testUtils.executeQuery(query)
                assert(testUtils.df_read(table_name).count == 1)
                val schema = StructType(Seq(StructField("col1", combo._3, true)))
                val data = Seq(Row(combo._4))
                val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
                testUtils.df_write(df, SaveMode.Append, table_name)
                log.debug(s"Appended successfully to table with col type ${combo._3}")
                assert(testUtils.df_read(table_name).count == 2)
                testUtils.drop_test_table(table_name)
            } catch {
                case e: Throwable => {
                    log.error(s"Error from data type/value combo ${combo}")
                    throw e
                }
            }
        }
    }

    // Test write with tablock not specified, set as true and false.
    def test_gci_tabLock_write() {
        val table_name = s"test_gci_tabLock_write${testType }"
        val df = testUtils.create_toy_df()
        testUtils.df_write(df, SaveMode.Overwrite, table_name)
        testUtils.df_write(df, SaveMode.Overwrite, table_name, tabLock = "true")
        testUtils.df_write(df, SaveMode.Append, table_name, tabLock = "false")

        var result = testUtils.df_read(table_name)
        assert(testUtils.compareSchemaIgnoreColsMetadata(df.schema, result.schema))
        assert(result.count == 2*df.count())
        log.info("test_gci_tabLock_write : Exit")
        testUtils.drop_test_table(table_name)
    }

    // Secure URL tests.
    def test_gci_secureURL_write() {
        val table_name = s"test_gci_secureURL_write${testType }"
        val df = testUtils.create_toy_df()
        testUtils.df_write(df, SaveMode.Overwrite, table_name, encrypt = "false", trustServerCertificate = "true")
        testUtils.df_write(df, SaveMode.Append, table_name, encrypt = "true", trustServerCertificate = "true")

        var result = testUtils.df_read(table_name)
        assert(testUtils.compareSchemaIgnoreColsMetadata(df.schema, result.schema))
        assert(result.count == 2*df.count())
        log.info("test_gci_secureURL_write : Exit")
        testUtils.drop_test_table(table_name)
    }

    def test_gci_reordered_columns() {
        val table_name = s"test_gci_reordered_columns${testType}"
        log.info("test_gci_reordered_columns : Created DF")

        // Create a dataframe with default column order i.e. 0,1,2,3,4
        val df = testUtils.dfTableUtility.create_df()

        // Create table with diffirent column order than  dataframe
        testUtils.dfTableUtility.create_table(table_name,List(1,0,4,3,2))
        log.info("test_gci_reordered_columns : Created table")
        testUtils.df_write(df, SaveMode.Append, table_name)
        log.info("test_gci_reordered_columns : Append succcessful")

        var result = testUtils.df_read(table_name)
        assert(df.count() == result.count())
        testUtils.dfTableUtility.check_ret_values(result)
        log.info("test_gci_reordered_columns : Read back table and confirmed data is added succcessful")

        val cols = testUtils.dfTableUtility.getDfCols(List(4,3,2,1,0))
        var df_reordered = df.select(cols.head, cols.tail: _*)
        testUtils.df_write(df_reordered, SaveMode.Overwrite, table_name, isTruncate = "true")
        result = testUtils.df_read(table_name)
        assert(df_reordered.count() == result.count())
        log.info("test_gci_reordered_columns : Reordered Write overwrite with truncate")

        testUtils.df_write(df_reordered, SaveMode.Append, table_name)
        result = testUtils.df_read(table_name)
        assert(2*df_reordered.count() == result.count())
        log.info("test_gci_reordered_columns : Reordered write append")

        // The step below finally overwrites the table in same order as data frame
        testUtils.df_write(df_reordered, SaveMode.Overwrite, table_name)
        result = testUtils.df_read(table_name)
        assert(df_reordered.count() == result.count())
        log.info("test_gci_reordered_columns : Reordered Write overwrite without truncate")
        testUtils.drop_test_table(table_name)
    }

    // Test basic functionalities of writing to different databases in parallel
    def test_gci_write_parallel() {
        //Allowing a maximum of 2 threads to run
        val executorService = Executors.newFixedThreadPool(2)
        implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

        val table_name1 = s"test_write_parallel_1_${testType }"
        val table_name2 = s"test_write_parallel_2_${testType }"
        val df = testUtils.create_toy_df()
        val futureA = Future {
            testUtils.df_write(df, SaveMode.Overwrite, table_name1)
        }
        val futureB = Future {
            testUtils.df_write(df, SaveMode.Overwrite, table_name2)
        }
        Await.result(futureA, Duration.Inf)
        Await.result(futureB, Duration.Inf)

        var result1 = testUtils.df_read(table_name1)
        assert(testUtils.compareSchemaIgnoreColsMetadata(df.schema, result1.schema))
        var result2 = testUtils.df_read(table_name2)
        assert(testUtils.compareSchemaIgnoreColsMetadata(df.schema, result2.schema))
        log.info("test_write_parallel : Exit")
        testUtils.drop_test_table(table_name1)
        testUtils.drop_test_table(table_name2)
    }
}
