# Databricks notebook source
# DBTITLE 1,Import Python ADAL Library
# Microsoft ADAL Library 
# The Python ADAL library will need to be installed.
# Example: 'pip install adal'

# Documentation
# https://github.com/AzureAD/azure-activedirectory-library-for-python

# Example
# https://github.com/AzureAD/azure-activedirectory-library-for-python/blob/dev/sample/client_credentials_sample.py

# Note, the Python ADAL library is no longer being maintained and it's documentation suggests to use MSAL. 
# Unfortunately, it doesn't have feature parity with adal at this point and does not support service principal authentication.
import adal

# COMMAND ----------

# DBTITLE 1,Configure Connection
# Located in App Registrations from Azure Portal
tenant_id = "72f988bf-86f1-41af-91ab-2d7cd011db47"

# Authority
authority = "https://login.windows.net/" + tenant_id

# Located in App Registrations from Azure Portal
resource_app_id_url = "https://database.windows.net/"

# In the below example, we're using a Databricks utility that facilitates acquiring secrets from 
# a configured Key Vault.  

# Service Principal Client ID - Created in App Registrations from Azure Portal
service_principal_id = dbutils.secrets.get("principalClientId")

# Service Principal Secret - Created in App Registrations from Azure Portal
service_principal_secret = dbutils.secrets.get("principalSecret")

# SQL Server URL
url = "jdbc:sqlserver://azuresqlserver.database.windows.net"

# Database Name
database_name = "TestDatabase"

# Database Table Name
db_table = "dbo.TestTable" 

# Encrypt
encrypt = "true"

# Host Name in Certificate
host_name_in_certificate = "*.database.windows.net"

# COMMAND ----------

# DBTITLE 1,Authentication Options
# SERVICE PRINCIPAL AUTHENTICATION
#   You will need to obtain an access token.
#   The "accessToken" option is used in the spark dataframe to indicate this 
#   authentication modality.
context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)

# Set Access Token
access_token = token["accessToken"]

# ACTIVE DIRECTORY PASSWORD AUTHENTICATION
#   The "authentication" option with the value of "ActiveDirectoryPassword"
#   is used in the spark dataframe to indicate this 
#   authentication modality. 
#
#   The "user" and "password" options apply to both SQL Authentication
#   and Active Directory Authentication.  SQL Authentication is used
#   by default and can be switched to Active Directory with the
#   authentication option above.
user = dbutils.secrets.get("adUser")
password = dbutils.secrets.get("adPassword")


# COMMAND ----------

# DBTITLE 1,Query SQL using Spark with Service Principal Token
jdbc_df = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
              .option("url", url) \
              .option("dbtable", db_table) \
              .option("accessToken", access_token) \
              .option("encrypt", encrypt) \
              .option("databaseName", database_name) \
              .option("hostNameInCertificate", host_name_in_certificate) \
              .load() 

display(jdbc_df.select("SourceViewName").limit(1))

# COMMAND ----------

# DBTITLE 1,Query SQL using Spark with Active Directory Password
jdbc_df = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
              .option("url", url) \
              .option("dbtable", db_table) \
              .option("authentication", "ActiveDirectoryPassword") \
              .option("user", user) \
              .option("password", password) \
              .option("encrypt", encrypt) \
              .option("databaseName", database_name) \
              .option("hostNameInCertificate", host_name_in_certificate) \
              .load() 

display(jdbc_df.select("SourceViewName").limit(1))
