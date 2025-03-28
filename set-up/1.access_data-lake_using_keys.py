# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC #### 1.Set the spark config fs.azure.account.key
# MAGIC #### 2.List files from demo container
# MAGIC #### 3.Read data from circuits.csv

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.mathiasf1datalake.dfs.core.windows.net",
    "MY_key"
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@mathiasf1datalake.dfs.core.windows.net"))

# COMMAND ----------

#Read data from circuits.csv
display(spark.read.csv("abfss://demo@mathiasf1datalake.dfs.core.windows.net"))

# COMMAND ----------

