import com.microsoft.aad.msal4j.ClientCredentialFactory
import com.microsoft.aad.msal4j.ClientCredentialParameters
import com.microsoft.aad.msal4j.ConfidentialClientApplication
import com.microsoft.aad.msal4j.IAuthenticationResult

import com.nimbusds.oauth2.sdk.http.HTTPResponse
import com.nimbusds.oauth2.sdk.id.ClientID

import java.util.Collections


val dbname = "yourdbname"
val servername = "jdbc:sqlserver://yourazsqlserver.database.windows.net"
val tablename = "yourtablename"


val authority="https://login.microsoftonline.com/yourtenantid"
val clientId="clientidofyourserviceprincipal"
val secret="clientsecret"
val scope="https://database.windows.net/.default"

val app = ConfidentialClientApplication.builder(
                clientId,
                ClientCredentialFactory.createFromSecret(secret))
                .authority(authority)
                .build()
// With client credentials flows the scope is ALWAYS of the shape "resource/.default", as the
// application permissions need to be set statically (in the portal), and then granted by a tenant administrator
val clientCredentialParam = ClientCredentialParameters.builder(
                Collections.singleton(scope))
                .build()

val accessToken=app.acquireToken(clientCredentialParam).get().accessToken()

val df = spark.read.format("com.microsoft.sqlserver.jdbc.spark")
  .option("url",servername)
  .option("dbtable",tablename)
  .option("databaseName",dbname)
  .option("accessToken",accessToken)
  .option("connectTimeout",30) //seconds, allow extra time for paused DB to start-up
  .option("queryTimeout",30)  //seconds
  .option("hostNameInCertificate","*.database.windows.net")
  .option("trustServerCertificate","true")
  .option("encrypt","true")
  .load()
display(df)

