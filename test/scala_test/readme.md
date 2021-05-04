# main class
SparkConnTestMain

# Parameters:
0. user : username
1. password : password
2. principal : service principal
3. keytab : keytab file path
4. dataSrcFormat: com.microsoft.sqlserver.jdbc.spark or jdbc
5. database : user database to use.
6. externalDataSource : external data source for data pool tests
7. suiteType : 0 for GCI(Gated CI test), 1 for CI test ( few additional test). Use 0 to run all regression test.
8. dataPoolTest : "true" or "false". Set as "false" if no data pool test should be run
9. Sqlservername : url for sql service
10. port : sql server port
11. domain : domain for AD test

# Usage
spark-submit --master yarn --deploy-mode cluster --class SparkConnTestMain /tmp/spark-mssql-connector-scala-test-assembly-1.0.0.jar 'connector_user' 'password123!#' '' '' 'com.microsoft.sqlserver.jdbc.spark' 'connector_test_db' 'spark_mssql_data_source' '0' 'false' 'master-p-svc' '1433' ''


