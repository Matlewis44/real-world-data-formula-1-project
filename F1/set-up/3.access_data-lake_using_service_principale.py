# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using service prncipale
# MAGIC #### Steps to follow
# MAGIC  1. Register azure AD Application /Service principal
# MAGIC  2. Generate a secret/ password for the Application
# MAGIC  3. Set Spark Config with App/Client id, Directory/Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_ID = service_credential = dbutils.secrets.get(scope="formula1-scope",key="formula1-client-ID")
tenant_ID = dbutils.secrets.get(scope="formula1-scope",key="formula1-tenant-Id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.mathiasf1datalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mathiasf1datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mathiasf1datalake.dfs.core.windows.net", client_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret.mathiasf1datalake.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mathiasf1datalake.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_ID}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@mathiasf1datalake.dfs.core.windows.net"))

# COMMAND ----------

#Read data from circuits.csv
display(spark.read.csv("abfss://demo@mathiasf1datalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

