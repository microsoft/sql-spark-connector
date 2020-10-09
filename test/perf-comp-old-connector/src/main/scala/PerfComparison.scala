import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object PerfComparison {
  val spark = SparkSession.builder().appName("Perf_Vs_Old").getOrCreate()

  def main(args:Array[String]) : Unit = {
    println("hello")

    val df = createDF()
    disectDF(df)

    time_old()
    time_new()


    def time_old() : Double = {
      import com.microsoft.azure.sqldb.spark.config.Config
      import com.microsoft.azure.sqldb.spark.connect._

      val url = "master-0.master-svc"
      val databaseName = "connector_test_db"
      val dbTable = "perf_old_table2"

      PreTestConfig.createIndexedTable(dbTable)
      PreTestConfig.trucateTable(dbTable)

      val user = "connector_user"
      val password = "password123!#"

      // WRITE FROM CONFIG
      val config = Config(Map(
        "url"            -> url,
        "databaseName"   -> databaseName,
        "dbTable"        -> dbTable,
        "user"           -> user,
        "password"       -> password,
        "bulkCopyBatchSize" -> "1048576",
        "bulkCopyTableLock" -> "False",
        "bulkCopyTimeout"   -> "7200"
      ))

      val start_table = System.nanoTime().toDouble
      //df_final.write.mode(SaveMode.Append).sqlDB(config)
      df.bulkCopyToSqlDB(config)
      val end_table = System.nanoTime().toDouble

      val run_time_table = (end_table - start_table) / 1000000000
      println("Time to write: " +   run_time_table)
      run_time_table
    }

    def time_new() : Double = {
      val servername = "jdbc:sqlserver://master-0.master-svc"
      val dbname = "connector_test_db"
      val url = servername + ";" + "databaseName=" + dbname + ";"

      val dbtable = "perf_new_table2"
      val user = "connector_user"
      val password = "password123!#"
      var start_table = System.nanoTime().toDouble

      PreTestConfig.createIndexedTable(dbtable)
      PreTestConfig.trucateTable(dbtable)

      df.write.
        format("com.microsoft.sqlserver.jdbc.spark").
        mode("append").
        option("url", url).
        option("dbtable", dbtable).
        option("user", user).
        option("password", password).
        option("truncate", true).
        option("schemaCheckEnabled", false).
        option("batchSize","1048576").
        option("tableLock", "false").
        save()
      var end_table = System.nanoTime().toDouble
      var run_time_table = (end_table - start_table) / 1000000000
      println("Time to write: " +  run_time_table)
      run_time_table
    }
  }

  def createDF() : DataFrame = {
    val read_schema_spec = StructType(Seq(
      StructField("ss_sold_date_sk", IntegerType, true),
      StructField("ss_sold_time_sk", IntegerType, true),
      StructField("ss_customer_sk", IntegerType, true),
      StructField("ss_cdemo_sk", IntegerType, true),
      StructField("ss_hdemo_sk", IntegerType, true),
      StructField("ss_addr_sk", IntegerType, true),
      StructField("ss_store_sk", IntegerType, true),
      StructField("ss_promo_sk", IntegerType, true),
      StructField("ss_cdemo_sk", IntegerType, true),
      StructField("ss_ticket_number", IntegerType, true),
      StructField("ss_quantity", IntegerType, true),
      StructField("ss_wholesale_cost", DecimalType(7,2), true),
      StructField("ss_list_price", DecimalType(7,2), true),
      StructField("ss_list_price", DecimalType(7,2), true),
      StructField("ss_sales_price", DecimalType(7,2), true),
      StructField("ss_ext_discount_amt", DecimalType(7,2), true),
      StructField("ss_ext_sales_price", DecimalType(7,2), true),
      StructField("ss_ext_wholesale_cost", DecimalType(7,2), true),
      StructField("ss_ext_list_price", DecimalType(7,2), true),
      StructField("ss_ext_tax", DecimalType(7,2), true),
      StructField("ss_coupon_amt", DecimalType(7,2), true),
      StructField("ss_net_paid", DecimalType(7,2), true),
      StructField("ss_net_paid_inc_tax", DecimalType(7,2), true),
      StructField("ss_net_profit", DecimalType(7,2), true)
    ))

    val path = "/user/testusera1/tpcds/datasets-1g/sf1-parquet/useDecimal=true,useDate=true,filterNull=false/store_sales"
    var df = spark.read.option("inferSchema","true").option("header","true").parquet(path)
    df = df.coalesce(1)
    df
  }

  def disectDF(df:DataFrame) : Unit = {
    df.printSchema()
    df.count()
    println("Nr of partitions is " + df.rdd.getNumPartitions)

    import org.apache.spark.util.SizeEstimator
    println(s"Size of dataframe is ${SizeEstimator.estimate(df)}")
  }

}

//val args  = Array[String]("2")
//PerfComparison.main(args)


