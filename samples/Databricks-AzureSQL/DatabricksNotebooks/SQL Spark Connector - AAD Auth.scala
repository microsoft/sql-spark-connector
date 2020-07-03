// Databricks notebook source
// DBTITLE 1,Import Microsoft ADAL Library
// Provides Access Token
import com.microsoft.aad.adal4j.{AuthenticationContext, ClientCredential}

// COMMAND ----------

// DBTITLE 1,Import Dependencies
import org.apache.spark.sql.SparkSession
import java.util.concurrent.Executors

// COMMAND ----------

// DBTITLE 1,Setup Connection Properties
val url = "jdbc:sqlserver://azuresqlserver.database.windows.net:1433;databaseName=TestDatabase"
val dbTable = "dbo.TestTable"

// In the below example, we're using a Databricks utility that facilitates acquiring secrets from 
// a configured Key Vault.  

// Service Principal Client ID - Created in App Registrations from Azure Portal
val principalClientId = dbutils.secrets.get("principalClientId")

// Service Principal Secret - Created in App Registrations from Azure Portal
val principalSecret = dbutils.secrets.get("principalSecret")

// Located in App Registrations from Azure Portal
val TenantId = "72f988bf-0000-0000-0000-00000000"

val authority = "https://login.windows.net/" + TenantId
val resourceAppIdURI = "https://database.windows.net/"

// COMMAND ----------

// DBTITLE 1,Acquire Access Token
val service = Executors.newFixedThreadPool(1)
val context = new AuthenticationContext(authority, true, service);
val ClientCred = new ClientCredential(spnId, spnSecret)
val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)

val accessToken = authResult.get().getAccessToken

// COMMAND ----------

// DBTITLE 1,Display a Spark Dataframe Showing Results from SQL
val jdbcDF = spark.read
    .format("com.microsoft.sqlserver.jdbc.spark")
    .option("url", url)
    .option("dbtable", dbTable)
    .option("accessToken", accessToken)
    .option("encrypt", "true")
    .option("hostNameInCertificate", "*.database.windows.net")
    .load()

  display(jdbcDF.select("SourceViewName").limit(1))
