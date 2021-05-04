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
import org.apache.spark.sql.types._
import java.sql.{Date, Timestamp}
/*
 * TypeCoversionToTest : define spark to sql types for test and validation.
 * MSSQL offers a fine granined system while spark type system is basic. Spark to SQL mapping defaults 
 * to the most generic types. e.g. StringType translates to VARCHAR(MAX) in SQL
 * How in-efficient is it? In MSSQL what's Varchar(max) // overhead v/s char(8). That's per row - Right?
*/

/*
 *  The Spark to SQL types are drevied from following Spark master as of 5/29/2019
 *
 *   From mssqlserverdialect.scala ( specialized types that apply to MSSQL)
 *   override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
 *       case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
 *       case StringType => Some(JdbcType("NVARCHAR(MAX)", java.sql.Types.NVARCHAR))
 *       case BooleanType => Some(JdbcType("BIT", java.sql.Types.BIT))
 *       case BinaryType => Some(JdbcType("VARBINARY(MAX)", java.sql.Types.VARBINARY))
 *       case _ => None
 *   }

 *   From jdbcutils.scala (default types) 
 *   def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
 *       dt match {
 *       case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
 *       case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
 *       case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
 *       case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
 *       case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
 *       case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
 *       case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
 *       case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
 *       case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
 *       case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
 *       case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
 *       case t: DecimalType => Option(
 *           JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
 *       case _ => None
 *       }
 *   }
*/

/*        
 * NOTES of some issues noted : 5/30/2019
 
ByteType : Exception during table creation

CREATE EXTERNAL TABLE test_data_types_10_bulk_dptest ("col1" BYTE ) WITH 
(DATA_SOURCE=test_data_types_datasource_10_bulk_dptest, DISTRIBUTION=ROUND_ROBIN);

ShortType : No problem creating table, when writing an exception occurs.
Caused by: java.lang.RuntimeException: Error while encoding: java.lang.RuntimeException:
java.lang.Integer is not a valid external type for schema of smallint
Following table creation is wrong. It should use smallint
("col1" INTEGER ) WITH (DATA_SOURCE=test_data_types_datasource_10_bulk_dptest, DISTRIBUTION=ROUND_ROBIN);


FloatType:: No problem creating table, when writing an exception occurs.
Table Creation stmt :
CREATE EXTERNAL TABLE test_data_types_10_bulk_dptest ("col1" REAL ) WITH (DATA_SOURCE=test_data_typ

Exception :        
Caused by: java.lang.RuntimeException: Error while encoding: java.lang.RuntimeException: java.lang.Double is not a valid external type for schema of float
        if (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 0, col1), FloatType) AS col1#60
    at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.toRow(ExpressionEncoder.scala:292)          ...

*/

object TypeCoversionToTest {    
    // spark_mssql_type_list_actual : Each tuple contains is Spark Type, JDBC Type, Equivalent SQL type, value that we use in the test
    // The list is used to verify all SparkTypes can be used to create equivant types in SQL via JDBC connection.
    val spark_mssql_type_list_actual = List(
        (LongType, "BIGINT","bigint", 2.toLong),
        (IntegerType,"INTEGER", "int", 2),
        (DecimalType(19, 4), "DECIMAL(19,4)","decimal(19, 4)", BigDecimal(245.6)),            
        (DecimalType(10, 3), "DECIMAL(10,3)","decimal(10, 3)", BigDecimal(9876540.321)),         
        (DecimalType(10, 4), "DECIMAL(10,4)","decimal(10, 4)", BigDecimal(1234.5678)),            
        (DoubleType,"DOUBLE PRECISION","float", 40.57), 
        (StringType,"NVARCHAR(MAX)" ,"nvarchar(max)", "Hello World!!"),
        (TimestampType,"DATETIME","datetime", Timestamp.valueOf("2017-12-25 21:43:56.987")),
        (DateType,"DATE","date", Date.valueOf("2017-12-25")), 
        (BooleanType,"BIT","bit", true),
        (BinaryType,"VARBINARY(MAX)","varbinary(6)", Array[Byte](745.toByte, 12)), // Fixed by SPARK-27159
        (ByteType,"tinyint","tinyint",1.toByte),
        (FloatType,"REAL","float",1.toFloat), // Fixed by SPARK-28152
        (ShortType,"smallint","smallint",12.toShort) // Fixed by SPARK-28152
    )

    // Each tuple contains SQL type, SQL value, Spark type, and Spark value
    // The list is used to verify that tables created in SQL can be read/appended to from Spark using right SQL
    // to Spark conversion mappings.
    val mssql_to_spark_type_create_table = List(
        ("bigint", "1", LongType, 2.toLong),
        ("binary(4)", "123", BinaryType, Array[Byte](192.toByte, 3)),
        ("bit", "0", BooleanType, true),
        ("char(8)", "'hello'", StringType, "world"),
        ("date", "'2018-01-31'", DateType, Date.valueOf("2017-12-25")),
        ("datetime", "'2018-01-31 12:34:56.789'", TimestampType, Timestamp.valueOf("2017-12-25 21:43:56.987")),
        ("datetime2", "'0987-01-31 12:34:56.789'", TimestampType, Timestamp.valueOf("5645-12-25 21:43:56.9876543")),
        ("datetimeoffset", "'0987-01-31 12:34:56.78987 +05:26'", StringType, "5645-12-25 21:43:56.98765 -12:38"),
        ("decimal(10, 3)", "1234560.789", DecimalType(10, 3), BigDecimal(9876540.321)),
        ("float", "60.24", DoubleType, 40.57),
        ("int", "1", IntegerType, 2),
        ("money", "456.7859", DecimalType(19, 4), BigDecimal(245.6)),
        ("nchar(8)", "'hello'", StringType, "world"),
        ("numeric(10, 3)", "1234560.789", DecimalType(10, 3), BigDecimal(9876540.321)),
        ("nvarchar(8)", "'hello'", StringType, "world"),
        ("smallint", "1", ShortType, 2.toShort),
        ("smallmoney", "123.45", DecimalType(10, 4), BigDecimal(1234.5678)),
        ("tinyint", "1", ByteType, 2.toByte),
        ("uniqueidentifier", "'01234567-89AB-CDEF-0123-456789ABCDEF'", StringType, "0E984725-C51C-4BF4-9960-E1C80E27ABA0"),
        ("varbinary(6)", "123456", BinaryType, Array[Byte](745.toByte, 12)),
        ("varchar(8)", "'hello'", StringType, "world"),
        ("text", "'hello'", StringType, "world"),
        ("ntext", "'hello'", StringType, "world"),
        ("image", "'123456'", BinaryType, Array[Byte](874.toByte, 100)),
        ("xml", "'hello'", StringType, "world"),
        ("nvarchar(max)", "'hello'", StringType, "world"),
        ("real", "60.24", FloatType, 60.24.toFloat)
    )

    // Each tuple contains SQL type, SQL value, Spark type, and Spark value
    // The list is used to verify that external tables created in SQL can be read/appended to from Spark using right SQL
    // to Spark conversion mappings.
    val mssql_to_spark_type_create_ex_table = List(
        ("bigint", "1", LongType, 2.toLong),
        ("binary(4)", "123", BinaryType, Array[Byte](192.toByte, 3)),
        ("bit", "0", BooleanType, true),
        ("char(8)", "'hello'", StringType, "world"),
        ("date", "'2018-01-31'", DateType, Date.valueOf("2017-12-25")),
        ("datetime", "'2018-01-31 12:34:56.789'", TimestampType, Timestamp.valueOf("2017-12-25 21:43:56.987")),
        ("datetime2", "'0987-01-31 12:34:56.789'", TimestampType, Timestamp.valueOf("5645-12-25 21:43:56.9876543")),
        ("datetimeoffset", "'0987-01-31 12:34:56.78987 +05:26'", StringType, "5645-12-25 21:43:56.98765 -12:38"),
        ("decimal(10, 3)", "1234560.789", DecimalType(10, 3), BigDecimal(9876540.321)),
        ("float", "60.24", DoubleType, 40.57),
        ("int", "1", IntegerType, 2),
        ("money", "456.7859", DecimalType(19, 4), BigDecimal(245.6)),
        ("nchar(8)", "'hello'", StringType, "world"),
        ("numeric(10, 3)", "1234560.789", DecimalType(10, 3), BigDecimal(9876540.321)),
        ("nvarchar(8)", "'hello'", StringType, "world"),
        ("smallint", "1", ShortType, 2.toShort),
        ("smallmoney", "123.45", DecimalType(10, 4), BigDecimal(1234.5678)),
        ("tinyint", "1", ByteType, 2.toByte),
        ("uniqueidentifier", "'01234567-89AB-CDEF-0123-456789ABCDEF'", StringType, "0E984725-C51C-4BF4-9960-E1C80E27ABA0"),
        ("varbinary(6)", "123456", BinaryType, Array[Byte](745.toByte, 12)),
        ("varchar(8)", "'hello'", StringType, "world"),
        ("real", "60.24", FloatType, 60.24.toFloat)
        // External table creation fails with following datatypes. Tests are commented for now.
        // ("text", "'hello'", StringType, "world"), // type 'text' is not supported with external tables for sharded data
        // ("ntext", "'hello'", StringType, "world"), // type 'text' is not supported with external tables for sharded data
        // ("image", "'123456'", BinaryType, Array[Byte](874.toByte, 100)),
        // ("xml", "'hello'", StringType, "world"), //The type 'xml' is not supported with external tables for sharded data.

        // External table creation passes , but insert to table fails with following datatypes. Tests are commented for now.
        // ("nvarchar(max)", "'hello'", StringType, "world") 
        // Err on insert : Cannot execute the query "Remote Query" against OLE DB provider "SQLNCLI11" for linked server 
        // "SQLNCLI11". 111234;The DMS native layer has encountered an error that caused this operation to fail. 
        // Details: Please use this Error ID when contacting your Administrator for assistance. EID:(a73153f2c57e4f77878c4da53f4b9fa6)
        // SqlNativeBufferBufferBulkCopy.WriteToServer, error in OdbcWriteBuffer: SqlState: , NativeError: 111234, 
        // 'COdbcBcpConnection::WriteBuffer, input buffer must be a multiple of m
    )
}
