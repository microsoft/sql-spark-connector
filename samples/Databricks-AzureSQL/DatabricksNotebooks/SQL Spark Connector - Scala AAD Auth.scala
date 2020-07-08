// Databricks notebook source
// DBTITLE 1,Import Microsoft ADAL Library
// REQUIREMENT - The com.microsoft.aad.adal4j artifact must be included as a dependency.
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

// DBTITLE 1,Authentication Options
// SERVICE PRINCIPAL AUTHENTICATION
//   You will need to obtain an access token.
//   The "accessToken" option is used in the spark dataframe to indicate this 
//   authentication modality.
val service = Executors.newFixedThreadPool(1)
val context = new AuthenticationContext(authority, true, service);
val ClientCred = new ClientCredential(principalClientId, principalSecret)
val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)

val accessToken = authResult.get().getAccessToken

// ACTIVE DIRECTORY PASSWORD AUTHENTICATION
//   The "authentication" option with the value of "ActiveDirectoryPassword"
//   is used in the spark dataframe to indicate this 
//   authentication modality. 
//
//   The "user" and "password" options apply to both SQL Authentication
//   and Active Directory Authentication.  SQL Authentication is used
//   by default and can be switched to Active Directory with the
//   authentication option above.
val user = dbutils.secrets.get("adUser")
val password = dbutils.secrets.get("adPassword")


// COMMAND ----------

// DBTITLE 1,Query SQL using Spark with Service Principal

val jdbcDF = spark.read
    .format("com.microsoft.sqlserver.jdbc.spark")
    .option("url", url)
    .option("dbtable", dbTable)
    .option("accessToken", accessToken)
    .option("encrypt", "true")
    .option("hostNameInCertificate", "*.database.windows.net")
    .load()

  display(jdbcDF.select("SourceViewName").limit(1))

// COMMAND ----------

// DBTITLE 1,Query SQL using Spark with Active Directory Password

val jdbcDF = spark.read
    .format("com.microsoft.sqlserver.jdbc.spark")
    .option("url", url)
    .option("dbtable", dbTable)
    .option("authentication", "ActiveDirectoryPassword")
    .option("user", user)
    .option("password", password)
    .option("encrypt", "true")
    .option("hostNameInCertificate", "*.database.windows.net")
    .load()

  display(jdbcDF.select("SourceViewName").limit(1))
