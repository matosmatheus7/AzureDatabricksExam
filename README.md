# Preparing to Start the Using SQL in Azure Databricks to Answer Business Questions Exam

This document prepares you to start the Using SQL in Azure Databricks to Answer Business Questions Exam. This exam requires the use of the Azure Databricks and Data Lake Storage Lab to answer the questions in the exam, the following sections will guide you through preparing the lab playground environment for the exam.

Questions:

1.  How many unique countries are there in the dataset?
2.  How many countries have the letter C (case-insensitive) in the name?
3.  How many customers do NOT belong to the WESTERN EUROPE region?
4.  What is the number of characters in the longest country’s name?
5.  Which region has the most countries in it?
6.  How many Customers belong to the OCEANIA region?
7.  Sort the country data by name. What is the first country to appear?
8.  What's the difference (mathematical) between the number of countries with lower-case "s" in their name and upper-case "S" in their name?
9.  How many countries have the letter C (case-insensitive) in the name?
10. How many regions are there in the countries dataset?

## How to setup Azure Databricks and Azure Data Lake Storage

The exam asks to answer questions about customer and country data using SQL. We will use Azure Data Lake Store as the data source for running queries with Databricks. We will upload data files containing the customer and country data to the Data Lake Store.

1\. Start and Create the Azure Databricks and Data Lake Storage.  
Ensure that you have that you have a running Azure Databricks cluster and a notebook ready to use.

2\. Upload the sample data files on the Data Lake Store via Azure Portal.

3\. Import data from Azure Data Lake Storage (ADLS) to Azure Databricks, so we can analyze and answer the exam questions.  
In Databricks notebook, enter the following command:

```
%fs ls adl://{YOUR DATA LAKE ACCOUNT NAME}.azuredatalakestore.net/
```

This command will list the files in the root of your data lake. Ensure the two JSON files are listed.  
Now we must mount the ADLS account into a /mnt/datalake/ directory in the notebook:

```
%python
configs = {
  "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
  "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
}
dbutils.fs.mount(
source = "adl://{YOUR DATA LAKE ACCOUNT NAME}.azuredatalakestore.net/",
mount_point = "/mnt/datalake",
extra_configs = configs)
```

By doing that we will be able to more easily access the data.

Now, let´s use the following SQL commands to import the data from the JSON files to SQL Tables:

```
%sql
CREATE TABLE countries
USING json
OPTIONS (
path "/mnt/datalake/countries.json"
)

%sql
CREATE TABLE customers
USING json
OPTIONS (
path "/mnt/datalake/customers.json"
)
```
