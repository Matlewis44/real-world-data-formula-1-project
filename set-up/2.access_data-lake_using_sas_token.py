# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using sas token
# MAGIC #### 1.Set the spark config for SAS token
# MAGIC #### 2.List files from demo container
# MAGIC #### 3.Read data from circuits.csv

# COMMAND ----------

datalake_security = dbutils.secrets.get(scope="formula1-scope", key="formula1-Sas-token-key-Vault")

spark.conf.set("fs.azure.account.auth.type.mathiasf1datalake.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.mathiasf1datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.mathiasf1datalake.dfs.core.windows.net", datalake_security)

# COMMAND ----------

# Non sécurisé

# spark.conf.set("fs.azure.account.auth.type.mathiasf1datalake.dfs.core.windows.net", "SAS")
# spark.conf.set("fs.azure.sas.token.provider.type.mathiasf1datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# spark.conf.set("fs.azure.sas.fixed.token.mathiasf1datalake.dfs.core.windows.net", "blob SAS token en dur")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@mathiasf1datalake.dfs.core.windows.net"))

# COMMAND ----------

#Read data from circuits.csv
display(spark.read.csv("abfss://demo@mathiasf1datalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

