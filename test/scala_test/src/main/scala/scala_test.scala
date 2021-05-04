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

import com.sun.org.apache.xpath.internal.functions.FuncFalse
import java.util.Properties
import java.sql.{Connection, Statement, DriverManager, Date, Timestamp}
import org.apache.spark.sql.{SparkSession, SaveMode, Row, DataFrame}
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types._
import scala.io.Source

/*
 * SparkConnTest
 * class that Drives the connector test execution. 
 * @ user : database user name
 * @ password : database user password
 * @ principal : AD user principal
 * @ keytab : AD user keytab name
 * @ dataSrcFormat : connector type to be used. 
 *     "JDBC" for default Spark connector. 
 *     "com.microsoft.sqlserver.jdbc.spark" for MSSQL-Spark connector
 * @ userDataBase : database name
 * @ externalDataSource : external data source for data pool
 * @ suiteType : run full suite or not.
 *      "0"   : Run GCI test case suite
 *      "1"   : Run CI Test case suite
 *      "2"   : Run Perf test case suite
 *      ..    : <Future expansion>
 *      "9"   : Run everything
 * @ dataPoolTest: run datapool test (true or false)
 * @ serviceName : sql server URL
 * @ servicePort : sql server port
 * @ domain : AD domain name
 * TODO: Change to dataSrcFormat to simpler name mssql-spark
 */
class SparkConnTest(val user: String, val password: String,
                    val principal: String, val keytab: String,
                    val dataSrcFormat: String, val userDataBase: String,
                    val externalDataSource: String = "",
                    val suiteType: String, val dataPoolTest:Boolean=true,
                    val serviceName: String, val servicePort : Int,
                    val domain: String = "") {
    // We assume that there are only two possible strings:
    // "jdbc" and "com.microsoft.sqlserver.jdbc.spark"
    val dataSrc = if (dataSrcFormat == "jdbc") "jdbc" else "bulk"
    val dataPoolName = "data-0"
    val runDataPoolTest = dataPoolTest

    // Defines the testcasePrefix to run
    val testSuitePrefix : String = {
      suiteType match {
        case "0" => "test_gci"
        case "1" => "test_ci"
        case "2" => "test_perf"
        case "9" => "test"
        case _ => "none"
      }
    }
    //name your app based on suite that will be run
    val testAppName = {
      s"MSSQLSparkConnector-Test-${dataSrcFormat}-${testSuitePrefix}"
    }

    val spark = SparkSession
                .builder()
                .master("yarn")
                .appName(testAppName)
                .getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")
    //Initialize the utils object. Utils takes care of creating a testdatabase if that does not exist.
    val testUtils = new Connector_TestUtils(
      spark, user, password, principal, keytab,
      dataSrcFormat, userDataBase, testSuitePrefix,
      externalDataSource, serviceName, servicePort, domain)

    def test_sqldatapools() {
        // dataSrc != bulk suggest jdbc test being run. Dont run datapools test as that's not supported
        if (dataSrc != "bulk" || runDataPoolTest != true) return
        testUtils.setReliableConnectorRun(false)
        val dpTestRunner = new DataPoolTests(testUtils)
        dpTestRunner.runner()
    }

    def test_sqlmaster() {
        // Note that master test can be run for both mssql-spark or jdbc connector.
        testUtils.setReliableConnectorRun(false)
        val masterTestRunner = new MasterInstanceTest(testUtils)
        masterTestRunner.runner()
    }

    def test_sqlmaster_reliable_connector() {
      // dataSrc != bulk suggest jdbc test being run. Dont run reliable connector test in that case.
      if(dataSrc!="bulk") return
      testUtils.setReliableConnectorRun(true)
      val masterReliableConnector = new MasterInstanceTest(testUtils)
      masterReliableConnector.runner()
  }
}

/*
 * SparkConnTestMain
 * Main class for the test jar.
 * @ arg0 : database user name
 * @ arg1 : database user password
 * @ arg2 : AD user principal
 * @ arg3 : AD user keytab name
 * @ arg4 : connector type to use ("JDBC" or "com.microsoft.sqlserver.jdbc.spark")
 * @ arg5 : database name
 * @ arg6 : data source for data pool
 * @ arg7 : test suite to run -GCI(0), CI(1), Perf(2), All(9)
 * @ arg8 : run datapool test (true or false)
 * @ arg9 : sql server URL
 * @ arg10: sql server port
 * @ arg11: AD domain
 * TODO: Change to dataSrcFormat to simpler name mssql-spark
 */
object SparkConnTestMain {
    def main(args: Array[String]) {
        val test_obj = new SparkConnTest(args(0), args(1), args(2), args(3), args(4),
          args(5), args(6), args(7), args(8).toBoolean, args(9), args(10).toInt, args(11) )
        val test_prefix = test_obj.getClass.getSimpleName + ".test_"
        for (method <- test_obj.getClass.getMethods) {
            if (method.toString.contains(test_prefix)) {
                method.invoke(test_obj)
            }
        }
        test_obj.spark.sparkContext.stop()
    }
}

//The following can be used to run test outside of GCI e.g manually or using this script in ADS
//val test_obj = new SparkConnTest("sa", "<pass>", "com.microsoft.sqlserver.jdbc.spark")
//test_obj.test_sqlmaster
//test_obj.test_sqldatapools
