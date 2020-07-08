/*
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.sqlserver.jdbc.spark

import java.sql.Connection

import com.microsoft.sqlserver.jdbc.spark.utils.{BulkCopyUtils, DataPoolUtils}
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

class DefaultSourceTest extends SparkFunSuite with Matchers with SharedSQLContext {

  test("Schema validation between Spark DataFrame and SQL Server ResultSet") {}

  test("JdbcBulkOptions should have proper Bulk configurations") {
    // The last character in each key is capitalized to test case insensitivity
    val params = Map(
      // Standard JdbcOptions configurations
      "urL" -> "jdbc:sqlserver://myUrl",
      "useR" -> "admin1",
      "passworD" -> "password1",
      "dbtablE" -> "myTable",
      "partitionColumN" -> "myPartitionColumn",
      "databaseNamE" -> "myDatabase",
      "accessTokeN" -> "1234",
      "encrypT" -> "true",
      "hostNameInCertificatE" -> "*.database.windows.net",
      "driverClasS" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "lowerBounD" -> "6",
      "upperBounD" -> "7",
      "numPartitionS" -> "1",
      "queryTimeouT" -> "2",
      "fetchSizE" -> "3",
      "truncatE" -> "true",
      "createTableOptionS" -> "myCreateTableOptions",
      "createTableColumnTypeS" -> "myCreateTableColumnTypes",
      "customSchemA" -> "myCustomSchema",
      "batchsizE" -> "4",
      "mssqlIsolationLeveL" -> "SERIALIZABLE",
      "sessionInitStatemenT" -> "mySessionInitStatement",
      "pushDownPredicatE" -> "false",
      // BulkCopy API configurations
      "checkConstraintS" -> "true",
      "fireTriggerS" -> "true",
      "keepIdentitY" -> "true",
      "keepNullS" -> "true",
      "tableLocK" -> "true",
      "allowEncryptedValueModificationS" -> "true",
      // New additions
      "reliabilityLeveL" -> "NO_DUPLICATES",
      "dataPoolDataSourcE" -> "testDataSource")

    val options = new SQLServerBulkJdbcOptions(params)
    assert(options.url == params("urL"))
    assert(options.user == params("useR"))
    assert(options.password == params("passworD"))
    assert(options.dbtable == params("dbtablE"))
    assert(options.databaseName == params("databaseNamE"))
    assert(options.accessToken == params("accessTokeN"))
    assert(options.encrypt == params("encrypT"))
    assert(options.hostNameInCertificate == params("hostNameInCertificatE"))
    assert(options.lowerBound.get == params("lowerBounD"))
    assert(options.upperBound.get == params("upperBounD"))
    assert(options.numPartitions.get == params("numPartitionS").toInt)
    assert(options.queryTimeout == params("queryTimeouT").toInt)
    assert(options.fetchSize == params("fetchSizE").toInt)
    assert(options.isTruncate == params("truncatE").toBoolean)
    assert(options.createTableOptions == params("createTableOptionS"))
    assert(options.createTableColumnTypes.get == params("createTableColumnTypeS"))
    assert(options.customSchema.get == params("customSchemA"))
    assert(options.batchSize == params("batchsizE").toInt)
    assert(options.sessionInitStatement.get == params("sessionInitStatemenT"))
    assert(options.pushDownPredicate == params("pushDownPredicatE").toBoolean)

    assert(options.checkConstraints == params("checkConstraintS").toBoolean)
    assert(options.fireTriggers == params("fireTriggerS").toBoolean)
    assert(options.keepIdentity == params("keepIdentitY").toBoolean)
    assert(options.keepNulls == params("keepNullS").toBoolean)
    assert(options.tableLock == params("tableLocK").toBoolean)
    assert(
      options.allowEncryptedValueModifications
        == params("allowEncryptedValueModificationS").toBoolean)

    assert(options.dataPoolDataSource == params("dataPoolDataSourcE"))
    assert(options.reliabilityLevel == SQLServerBulkJdbcOptions.NO_DUPLICATES)
    assert(options.isolationLevel == Connection.TRANSACTION_SERIALIZABLE)
    assert(options.driverClass == params("driverClasS"))
  }

  test("Data pool URL generation") {
    val urlParams =
      "database=spark_mssql_db;user=testusera1;password=mypass" +
        ";encrypt=false;trustServerCertificate=true;"
    val masterUrl = createURL("master-pv", "5678", urlParams)
    val params = Map("urL" -> masterUrl, "dbtablE" -> "myTable")
    val options = new SQLServerBulkJdbcOptions(params)

    val dphostName = "dp-0-01"
    val actualDpURL = createURL(dphostName, "1433", urlParams)
    val generatedDpUrl = DataPoolUtils.createDataPoolURL(dphostName, options)
    assert(actualDpURL == generatedDpUrl)
  }

  test("Multi part tablename test") {
    val testTableName = "myTable"
    val testSchemaName = "mySchema"
    val testDBName = "mydb"

    val urlParams = s"database=$testDBName;user=testusera1;password=mypass;"
    val masterUrl = createURL("master-pv", "5678", urlParams)

    // Test 1 - Check single part table name
    var params = Map("urL" -> masterUrl, "dbtable" -> s"$testTableName")
    var options = new SQLServerBulkJdbcOptions(params)
    val (rDb, rSchema, rTable) = BulkCopyUtils.get3PartName(options)
    assert(rDb == "")
    assert(rSchema == "dbo")
    assert(rTable == testTableName)

    // Test 2 - Check 2 part names
    params = Map("urL" -> masterUrl, "dbtable" -> s"$testSchemaName.$testTableName")
    options = new SQLServerBulkJdbcOptions(params)
    val (db, schema, table) = BulkCopyUtils.get3PartName(options)
    assert(db == "")
    assert(schema == testSchemaName)
    assert(table == testTableName)
  }

  def createURL(instanceName: String, port: String, urlParams: String): String = {
    s"jdbc:sqlserver://$instanceName:$port;$urlParams"
  }

  test("Data pool options test") {
    // DataPool is not configured.
    var options =
      new SQLServerBulkJdbcOptions(Map("urL" -> "jdbc:sqlserver://myUrl", "dbtablE" -> "myTable"))
    assert(false == DataPoolUtils.isDataPoolScenario(options))

    options = new SQLServerBulkJdbcOptions(
      Map("urL" -> "jdbc:sqlserver://myUrl", "dbtablE" -> "myTable", "dataPoolDataSource" -> ""))
    assert(false == DataPoolUtils.isDataPoolScenario(options))

    // DataPool is configured correctly
    options = new SQLServerBulkJdbcOptions(
      Map(
        "urL" -> "jdbc:sqlserver://myUrl",
        "dbtablE" -> "myTable",
        "dataPoolDataSource" -> "myds"))
    assert(true == DataPoolUtils.isDataPoolScenario(options))
  }

  test("Correct AAD driver is set when accessToken is specified") {
    val options = new SQLServerBulkJdbcOptions(
      Map("urL" -> "jdbc:sqlserver://myUrl", "dbtablE" -> "myTable", "accessToken" -> "1234"))
    options.driverClass should be("com.microsoft.sqlserver.jdbc.SQLServerDriver")
  }

}