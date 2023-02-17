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
import java.security.PrivilegedAction
import java.sql.{Connection, Date, DriverManager, Statement, Timestamp}
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import scala.language.postfixOps

/*
 * Connector_TestUtils
 * @ spark : spark Session to use
 * @ username : database user name
 * @ pass : database password
 * @ principal : AD user principal
 * @ keytab : AD user keytab name
 * @ format_option : connector type to be used.
 *       "JDBC" for default Spark connector. 
 *       "com.microsoft.sqlserver.jdbc.spark" for MSSQL-Spark connector
 * @ testDbName : test database name
 * @ testSuitePrefix : the prefix of test suite
 * @ externalDataSource : external data source for data pool
 * @ sqlHostName : host name of the sql server master instance
 * @ sqlPort : sql server port
 * @ domain: AD domain name
 * Collects all utility functions for test at the same place.
 */
class Connector_TestUtils(spark:SparkSession,
                          username: String,
                          pass: String,
                          principal: String,
                          keytab: String,
                          format_option: String,
                          testDbName: String,
                          testSuitePrefix: String,
                          externalDataSource: String,
                          sqlHostName: String,
                          sqlPort: Int,
                          domain: String) {
    val spark_session = spark
    val user = username
    val password = pass
    val dataPoolExternalDataSource = externalDataSource
    val dataSrcFormat = format_option
    val testType = if (dataSrcFormat == "jdbc") "jdbc" else "bulk"
    val testCasePrefix = testSuitePrefix
    val servicename = sqlHostName
    val port = sqlPort

    val logger = createModuleLogger("Connector_TestUtils")
    var runWithReliableConnector = false

    val hostname = {
        if (domain != null && domain != "") {
            servicename + "." + domain
        } else {
            servicename
        }
    }

    def createModuleLogger(moduleName:String) : Logger = {
        val logger = Logger.getLogger(moduleName)
        logger.setLevel(Level.INFO)
        logger
    }

    def sparkJDBCUrl(hostname:String, port:Int, encrypt:String="na",trustServerCertificate:String="na") : String =  {
        val securitySpec = {
            if (principal != "" && keytab != "") {
                s"integratedSecurity=true;authenticationScheme=JavaKerberos;"
                //s"jdbc:sqlserver://${hostname}:${port};database=${testDbName};integratedSecurity=true;authenticationScheme=JavaKerberos;"
            } else {
                s"user=${user};password=${password};"
                //s"jdbc:sqlserver://${hostname}:${port};database=${testDbName};user=${user};password=${password};"
            }
        }
        val encryptSpec = {
            encrypt match {
                case "na" => ""
                case _ => s"encrypt=$encrypt;"
            }
        }

        val trustSpec = {
            trustServerCertificate match {
                case "na" => ""
                case _ => s"trustServerCertificate=$trustServerCertificate;"
            }
        }

        // Construct URL and return
        s"jdbc:sqlserver://${hostname}:${port};database=${testDbName};$securitySpec$encryptSpec$trustSpec"
    }

    var conn = createConnection(hostname, port)

    def createConnection(hostname:String, port:Int) : Connection = {
        if (principal != "" && keytab != "") {
            UserGroupInformation
                .loginUserFromKeytabAndReturnUGI(principal, keytab)
                .doAs(new PrivilegedAction[Connection] {
                    override def run(): Connection = {
                        DriverManager.getConnection(sparkJDBCUrl(hostname, port))
                    }
                })
        } else {
            DriverManager.getConnection(sparkJDBCUrl(hostname, port))
        }
    }
    var stmt = conn.createStatement()

    def  dataPoolConnectionMap(): Map[String,Connection] = {
        var dpConnMap = scala.collection.mutable.Map[String,Connection]()
        getDataPoolNodes().foreach(dpHostName=> {
            dpConnMap(dpHostName) = createConnection(dpHostName,port)
        })
        dpConnMap.toMap
    }

    //Create a schema in the database
    val schemaName = "mssqlspark"
    createSchema(schemaName)

    /*
     * Utility functions defined below. 
     * These are common functions used by the test cases
     */

    def getSparkSession() : SparkSession = {
        spark
    }
    
    // Utility functions for basic database operations.
    def drop_test_table(name: String) {
        val drop_query = s"DROP TABLE IF EXISTS ${name};"
        stmt.executeUpdate(drop_query)
    }

    def drop_external_table(table_name:String) {
        val drop_query = s"DROP EXTERNAL TABLE ${table_name};"    
        stmt.executeUpdate(drop_query)               
    }

    def drop_datasource(dataSrcName:String) {
         val drop_query = s"DROP EXTERNAL DATA SOURCE ${dataSrcName};"
         stmt.executeUpdate(drop_query)
    }

    def executeQuery(query:String) {
        stmt.executeUpdate(query)
    }

    def getTestType() : String = {
        testType
    }

    def getDataPoolNodes() : List[String] = {
        import scala.collection.mutable.ListBuffer
        var node_list = new ListBuffer[String]()
        val drop_query = s"select address from sys.dm_db_data_pool_nodes"
        try {
            val rs = stmt.executeQuery(drop_query)
            while(rs.next()) {
                val dp_node = rs.getString("address")
                node_list += dp_node
            }
        } catch  {
            case ex: Exception => {
                logger.info(s"Failed with exception ${ex.getMessage()} \n")
            }
        }
       node_list.toList
    }
    
    /* Utility functions for read and write. Uses format as set in dataSrcFormat.
     * So will use JDBC connector or MSSQLSpark connector based on dataSrcFormat setting.
     * TODO : setting bulk parameters like reliabilitylevel on JDBC conenctor is wrong. JDBC ingores 
     * this and thus this is working. These functions need refactoring to have seperate 
     * functions for JDBC and mssqlspark connector.
     * TODO : pass dataSrcFormat as parameter to remove depedency on object variable.
     */    
    def df_write(df: DataFrame, 
                 mode: SaveMode, 
                 table_name: String, 
                 isTruncate: String="false",
                 isoLevel: String="READ_COMMITTED",
                 reliabilityLevel: String="BEST_EFFORT",
                 dataPoolDataSource: String="",
                 tabLock:String="na",
                 encrypt:String ="na",
                 trustServerCertificate:String="na",
                 datapoolDistPolicy:String="ROUND_ROBIN"): Unit = {
        val constructedURL = sparkJDBCUrl(hostname, port , encrypt,trustServerCertificate)
        logger.info(s"df_write: write with URL as $constructedURL")
        var df_write_options = Map("url" -> constructedURL,
                                   "dbtable" -> table_name,
                                   "truncate" -> isTruncate,
                                   "mssqlIsolationLevel" -> isoLevel,
                                   "reliabilityLevel" -> reliabilityLevel)


        if(dataPoolDataSource!="") {
            df_write_options =  df_write_options + ("dataPoolDataSource" -> dataPoolDataSource)
        }

        if(runWithReliableConnector) {
            df_write_options =  df_write_options + ("reliabilityLevel" -> "NO_DUPLICATES",
                                                    "testDataIdempotency" -> "true")
        } else {
            df_write_options = df_write_options + ("reliabilityLevel" -> "BEST_EFFORT")
        }

        tabLock match {
            case "true" =>  df_write_options = df_write_options + ("tableLock" -> "true")
            case "false" => df_write_options = df_write_options + ("tableLock" -> "false")
            case _ => logger.info(s"Not setting tableLock option \n")
        }

        if (principal != "" && keytab != "") {
            df_write_options = df_write_options + ("principal" -> principal, "keytab" -> keytab)
        } else {
            df_write_options = df_write_options + ("user" -> user, "password" -> password)
        }

        logger.info(s"datapoolDistPolicy is ${datapoolDistPolicy} \n")

        if(datapoolDistPolicy=="REPLICATED") {
            df_write_options = df_write_options + ("dataPoolDistPolicy" -> datapoolDistPolicy)
        }

        df.write.mode(mode).format(dataSrcFormat).options(df_write_options).save()
    }
        
    def df_read(table_name: String,
                dataPoolDataSource: String=""): DataFrame = {
        var df_read_options = Map("url" -> sparkJDBCUrl(hostname, port),
                                  "dbtable" -> table_name,
                                  "dataPoolDataSource" -> dataPoolDataSource)

        if (principal != "" && keytab != "") {
            df_read_options = df_read_options + ("principal" -> principal, "keytab" -> keytab)
        } else {
            df_read_options = df_read_options + ("user" -> user, "password" -> password)
        }

        spark_session.read.format(dataSrcFormat).options(df_read_options).load()
    }

    def get_create_query(table_name: String, data_type: String, value: String): String = {
        s"""DROP TABLE IF EXISTS ${table_name}; 
            CREATE TABLE ${table_name}(col1 ${data_type}); 
            INSERT INTO ${table_name} VALUES (${value});"""
    }

    def get_create_datasource(dataSrcName:String) : String = {
        val externalDataPool = "sqldatapool://controller-svc/default"
        s"CREATE EXTERNAL DATA SOURCE ${dataSrcName} WITH (LOCATION = '${externalDataPool}');"
    }
    def get_create_ex_table_query(table_name: String, datasource:String, data_type: String, value: String): String = {
        s"""CREATE EXTERNAL TABLE ${table_name}(col1 ${data_type}) WITH (DATA_SOURCE=${datasource}, DISTRIBUTION=ROUND_ROBIN);
            INSERT INTO ${table_name} VALUES (${value});"""
    }

    // Auxiliary function for creating a small dataframe
    def create_toy_df(): DataFrame = {
        val schema = StructType(Seq(
            StructField("entry_number", IntegerType, true),
            StructField("entry_word", StringType, true)
        ))
        val data = Seq(
            Row(24, "hello"),
            Row(43, "world"),
            Row(99, "prose"),
            Row(111, "ninja"),
            Row(149, "monitor")
        )
        spark_session.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    def createSchema(schemaName: String) : Unit = {
        logger.info(s"createSchema Entered \n")
        val queryStr = s"CREATE SCHEMA $schemaName"
        logger.info(s"queryStr is $queryStr \n")
        try {
            stmt.execute(queryStr)
        }
        catch {
            case ex: Exception => {
                logger.info(s"Create schema failed with message ${ex.getMessage()} \n")
            }
        }
        finally {
            logger.info("createSchema Existing \n")
        }
    }

    def createThreePartName(tableName:String) : String = {
        s"${testDbName}.${schemaName}.${tableName}"
    }

    def createTwoPartName(tableName:String) : String = {
        s"${schemaName}.${tableName}"
    }

    def setReliableConnectorRun(flag: Boolean) : Unit = {
        runWithReliableConnector = flag
    }

    // The new spark jdbc reads automatically fill metadata field
    // This method empty the metadata of column schema and do the comparison
    def compareSchemaIgnoreColsMetadata(df_schema:StructType, result_schema:StructType) : Boolean = {
        val result_schema_cleaned = StructType(result_schema.map(_.copy(metadata = Metadata.empty)))
        df_schema == result_schema_cleaned
    }

    object dfTableUtility {
        val table_cols = List (
            ("RecordTime", IntegerType, "int", true),
            ("Geography", StringType, "nvarchar(max)", true),
            ("WarningLevel", StringType, "nvarchar(max)", true),
            ("Trend", StringType, "nvarchar(max)", true),
            ("RecoveryMonth", IntegerType, "int", true)
        )

        val table_data = List(
            (201912,"china", "level1", "uptick", 202001),
            (202001,"china", "level2", "uptick", 202002),
            (202002,"Italy", "level3", "uptick", 202003),
            (201903,"china", "level1", "downtick", 202001),
            (202003,"Italy", "level3", "uptick", 202005),
            (202003,"USA", "level3", "uptick", 202005),
            (202004,"World", "level3", "uptick", 202008)
        )

        def create_df(): DataFrame = {
            val schema = StructType(Seq(
                StructField(table_cols(0)._1,table_cols(0)._2, table_cols(0)._4),
                StructField(table_cols(1)._1,table_cols(1)._2, table_cols(1)._4),
                StructField(table_cols(2)._1,table_cols(2)._2, table_cols(2)._4),
                StructField(table_cols(3)._1,table_cols(3)._2, table_cols(3)._4),
                StructField(table_cols(4)._1,table_cols(4)._2, table_cols(4)._4)
            ))
            val data = Seq(
                Row(table_data(0)_1,table_data(0)_2, table_data(0)_3, table_data(0)_4, table_data(0)_5),
                Row(table_data(1)_1,table_data(1)_2, table_data(1)_3, table_data(1)_4, table_data(1)_5),
                Row(table_data(2)_1,table_data(2)_2, table_data(2)_3, table_data(2)_4, table_data(2)_5),
                Row(table_data(3)_1,table_data(3)_2, table_data(3)_3, table_data(3)_4, table_data(3)_5),
                Row(table_data(4)_1,table_data(4)_2, table_data(4)_3, table_data(4)_4, table_data(4)_5),
                Row(table_data(5)_1,table_data(5)_2, table_data(5)_3, table_data(5)_4, table_data(5)_5),
                Row(table_data(6)_1,table_data(6)_2, table_data(6)_3, table_data(6)_4, table_data(6)_5)
            )
            spark_session.createDataFrame(spark.sparkContext.parallelize(data), schema)
        }

        def getDfCols(col_order:List[Int]) : List[String] = {
            col_order.map(index => {
                table_cols(index)_1
            })
        }

        def create_table(table_name:String, col_order:List[Int]=List(0,1,2,3,4)) : Unit = {
            val query_cols = col_order.map(i=> s" [${table_cols(i)_1}] ${table_cols(i)_3}").mkString(",")
            val create_table_query = s"CREATE TABLE $table_name ($query_cols)"
            executeQuery(s"DROP TABLE IF EXISTS ${table_name}; $create_table_query")
        }

        def create_external_table(table_name:String, col_order:List[Int], datasource:String) : Unit = {
            try{
                drop_external_table(table_name)
            } catch  {
                case ex: Exception => {
                    logger.info(s"Drop table excepection ${ex.getMessage()} \n")
                }
            }
            val query_cols = col_order.map(i=> s" [${table_cols(i)_1}] ${table_cols(i)_3}").mkString(",")
            val create_extable_query =  s"CREATE EXTERNAL TABLE ${table_name} (${query_cols})" +
                                           s"WITH (DATA_SOURCE=${datasource}, DISTRIBUTION=ROUND_ROBIN);"
            executeQuery(s"$create_extable_query")
        }

        def check_ret_values(result:DataFrame) : Unit = {
            List(0,1,2,3,4).map(index => {
                val col_result = result.select(table_cols(index)._1).collect().map(_(0)).toList
                val col_preset = table_data.map(tup => {
                    val l = tup.productIterator.toList
                    val i_value = l(index)
                    logger.info(s"tup is $tup. Extract value at index $index is $i_value")
                    i_value
                })
                logger.info(s"col_result : col ${table_cols(index)._1} : ${col_result.mkString(",")}")
                logger.info(s"col_preset : col ${table_cols(index)._1} : ${col_preset.mkString(",")}")
                col_preset.map(item => {
                    if(!col_result.contains(item)) {
                        // Expected value not found. Fail
                        assert(1 == 0)
                    }
                })
            })
        }
    }
}
