The objective of this solution is to showcase a use case to demonstrate the integration between Azure Databricks and Azure SQL Managed Instance to deliver insights and data visualizations using a publicly available COVID-19 dataset. While Azure Databricks provides the distributed computing power to process and transform complex datasets, Azure SQL Managed Instance acts as a recipient of the transformed dataset and serves it to business users. In doing so, the solution uses a publicly available COVID-19 dataset and runs a machine learning model in Databricks to predict the fatalities which are then written into a datamart in Azure SQL Managed Instance for visualization and reporting.

> Note: Gradient Boosting Trees (GBT) Regression model has been used in
> the solution purely for demonstration purposes and by no means should
> this be considered as the only machine learning algorithm to run
> predictions on the dataset used.


**Azure SQL Spark Connector**

The [Spark connector](https://docs.microsoft.com/en-us/azure/azure-sql/database/spark-connector) enables databases in Azure SQL Database, Azure SQL Managed Instance, and SQL Server to act as the input data source or output data sink for Spark jobs. The Spark connector supports Azure Active Directory (Azure AD) authentication to connect to Azure SQL Database and Azure SQL Managed Instance and it utilizes the Microsoft JDBC Driver for SQL Server to move data between Spark worker nodes and databases.

The connector can either be downloaded from the [azure-sqldb-spark GitHub repo](https://github.com/Azure/azure-sqldb-spark/tree/master/releases/azure-sqldb-spark-1.0.0) or imported into Azure Databricks using the Maven coordinate: `com.microsoft.azure:azure-sqldb-spark:1.0.2`

**Solution Architecture**

The solution architecture shown below depicts the various phases of data flow along with a variety of data sources and data consumers involved.

![Solution Architecture Image](https://github.com/mokabiru/databrickssqlmi/raw/master/media/Solution%20Architecture%20Numbered%20.jpg)

The solution comprises of the following parts as described in the data flow below (the sequence numbers are highlighted in the architecture diagram above)

 1. The solution extracts the COVID-19 public dataset available in a
    data lake (Azure Storage – Blob / ADLS Gen2) into Azure Databricks
    as a dataframe.
  2. The extracted COVID-19 dataset is cleaned, pre-processed, trained
    and scored using a Gradient Boosted Trees (GBT) Machine Learning
    model.

> *GBT is chosen to predict the deaths on a given day in a given country
> purely for   demonstration purposes only and should not
>     be considered as the only model for such prediction.*

3. The resulting dataset with the predicted scores is stored into a
staging table in Azure SQL Managed Instance for further downstream
transformation.
4. Common data dimension tables and the staging table (in Step 3) from
Azure SQL Managed Instance are read into dataframes in Azure
Databricks.

> The two Managed Instances shown in the “Store” and the “Serve” layer
> are essentially the same instance just depicted in different phases of
> the data flow. In a real-world, Azure SQL Managed Instance or Azure
> SQL Databases can play the role of both a data storage service and
> data serving service for consuming applications / data visualization
> tools.

5. The dataframes containing the necessary dimension and staging data
are further refined, joined and transformed to produce a
denormalised fact table for reporting.
6. The resulting denormalised data table is written to Azure SQL
Managed Instance ready to serve the data consumers.

**Azure Services:**
The solution requires the following Azure Services and their relevant SKUs.
1. **Azure Databricks Standard / Premium Workspace**
*Cluster Options: Databricks Runtime Version 6.5; Standard Mode; 2 Worker nodes and 1 Driver Node;
Node specification: Standard_DS3_v2*
2. **Azure SQL Managed Instance Business Critical 4vCores** (General Purpose can also be used). Business Critical is used for demonstration as it comes with a built-in Read Only replica that can be used for data reads / reporting.
3. **Azure Key Vault (Standard)** – to save secrets such as SQL Managed Instance login password

The solution is divided into two parts in two Azure Databricks notebooks. Click on the links below to dive deeper into each part of the solution. <br>
[**Part 1 walkthrough**](https://github.com/mokabiru/databrickssqlmi/blob/master/Part1_README.md): 
Notebook 1 ([Covid19-DatabricksML-SQLMI](https://github.com/mokabiru/databrickssqlmi/blob/master/DatabricksNotebooks/Covid19-DatabricksML-SQLMI.dbc)) emphasizes on the machine learning model training and prediction that executes Steps 1-3 in the data flow described above.

[**Part 2 walkthrough**](https://github.com/mokabiru/databrickssqlmi/blob/master/Part2_README.md):
Notebook 2 ([COVID19-Load-SQLManagedInstance](https://github.com/mokabiru/databrickssqlmi/blob/master/DatabricksNotebooks/COVID19-Load-SQLManagedInstance.dbc)) emphasizes on reading and writing data to Azure SQL Managed Instance using the Spark AzureSQL Connector that executes steps 4-6 in the data flow above.

**Pre-requisites steps:**<br>**1. Install Spark Connector for Azure SQL Database:**<br>Install the [Spark AzureSQLDB connector library](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/sql-databases-azure) (`azure-sqldb-spark`) on the Databricks cluster either by manually downloading and uploading the [.jar file](https://github.com/Azure/azure-sqldb-spark/tree/master/releases/azure-sqldb-spark-1.0.0) or importing the Maven coordinate `com.microsoft.azure:azure-sqldb-spark:1.0.2` as shown below:
![enter image description here](https://github.com/mokabiru/databrickssqlmi/raw/master/media/installlibrary.png)
![enter image description here](https://github.com/mokabiru/databrickssqlmi/raw/master/media/maveninstall.png)

**2. Prepare SQL MI Datamart:**<br>A sample BACPAC file `Covid19datamart.bacpac` is provided [here](https://github.com/mokabiru/databrickssqlmi/tree/master/SQLMI/bacpac) to import and create the datamart in SQL MI. Any existing data in Staging or Fact table needs to be deleted as the solution will insert a new dataset from the data lake. After importing the file from the GitHub repo to create the datamart in Azure SQL Managed Instance, run the following stored procedure on the imported datamart:

    EXEC sp_cleanuptables ‘all’
