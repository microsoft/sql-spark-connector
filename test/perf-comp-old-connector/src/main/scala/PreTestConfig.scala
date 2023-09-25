import java.sql.DriverManager

object PreTestConfig {
  val url = {
    val hostname = "master-0.master-svc"
    val port = 1433
    val testDbName = "connector_test_db"
    val user = "connector_user"
    val password = "password123!#"

    s"jdbc:sqlserver://${hostname}:${port};database=${testDbName};user=${user};password=${password}"
  }

  println(s"url to connect to is $url")
  val conn = DriverManager.getConnection(url)

  def trucateTable(name:String) : Unit = {
    val truncateTableStmt:String =
      s"""
        |TRUNCATE table dbo.${name}
        |""".stripMargin

    val stmt = conn.createStatement()
    stmt.executeUpdate(truncateTableStmt)
  }

  def createIndexedTable(name:String) : Unit = {
    val createTableStmt:String =
      s"""
         |CREATE TABLE [dbo].[${name}](
         |[ss_sold_date_sk] [int] NULL,
         |[ss_sold_time_sk] [int] NULL,
         |[ss_item_sk] [int] NOT NULL,
         |[ss_customer_sk] [int] NULL,
         |[ss_cdemo_sk] [int] NULL,
         |[ss_hdemo_sk] [int] NULL,
         |[ss_addr_sk] [int] NULL,
         |[ss_store_sk] [int] NULL,
         |[ss_promo_sk] [int] NULL,
         |[ss_ticket_number] [bigint] NOT NULL,
         |[ss_quantity] [int] NULL,
         |[ss_wholesale_cost] [decimal](7, 2) NULL,
         |[ss_list_price] [decimal](7, 2) NULL,
         |[ss_sales_price] [decimal](7, 2) NULL,
         |[ss_ext_discount_amt] [decimal](7, 2) NULL,
         |[ss_ext_sales_price] [decimal](7, 2) NULL,
         |[ss_ext_wholesale_cost] [decimal](7, 2) NULL,
         |[ss_ext_list_price] [decimal](7, 2) NULL,
         |[ss_ext_tax] [decimal](7, 2) NULL,
         |[ss_coupon_amt] [decimal](7, 2) NULL,
         |[ss_net_paid] [decimal](7, 2) NULL,
         |[ss_net_paid_inc_tax] [decimal](7, 2) NULL,
         |[ss_net_profit] [decimal](7, 2) NULL,
         |PRIMARY KEY CLUSTERED
         |(
         |[ss_item_sk] ASC,
         |[ss_ticket_number] ASC
         |)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
         |) ON [PRIMARY]
         |""".stripMargin

    val stmt = conn.createStatement()
    stmt.executeUpdate(createTableStmt)

    val createIndexStmt =
      s"""
        |CREATE INDEX idx_store_sales_s_store_sk ON dbo.${name} (ss_store_sk) INCLUDE (
        |ss_sold_date_sk
        |,ss_sold_time_sk
        |,ss_item_sk
        |,ss_customer_sk
        |,ss_cdemo_sk
        |,ss_hdemo_sk
        |,ss_addr_sk
        |,ss_promo_sk
        |,ss_ticket_number
        |,ss_quantity
        |,ss_wholesale_cost
        |,ss_list_price
        |,ss_sales_price
        |,ss_ext_discount_amt
        |,ss_ext_sales_price
        |,ss_ext_wholesale_cost
        |,ss_ext_list_price
        |,ss_ext_tax
        |,ss_coupon_amt
        |,ss_net_paid
        |,ss_net_paid_inc_tax
        |,ss_net_profit
        |)
        |""".stripMargin

    stmt.executeUpdate(createIndexStmt)

  }

}
