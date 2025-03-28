# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using service prncipale
# MAGIC #### Steps to follow
# MAGIC  1. Get Client_id, Tenant_Id & client_secret from key Vault
# MAGIC  2. Set Spark Config with App/Client id, Directory/Tenant Id & Secret
# MAGIC  3. Call file system utility mount to mount the storage
# MAGIC  4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_ID = service_credential = dbutils.secrets.get(scope="formula1-scope",key="formula1-client-ID")
tenant_ID = dbutils.secrets.get(scope="formula1-scope",key="formula1-tenant-Id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_ID,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_ID}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@mathiasf1datalake.dfs.core.windows.net/",
  mount_point = "/mnt/mathiasf1datalake/demo", #sur quel container et storage-account on va pointer
  extra_configs = configs)

# COMMAND ----------

# abfss protocole
# display(dbutils.fs.ls("abfss://demo@mathiasf1datalake.dfs.core.windows.net"))

# Mount
display(dbutils.fs.ls("/mnt/mathiasf1datalake/demo"))

# COMMAND ----------

#Read data from circuits.csv avec 
# protocole abfss
# display(spark.read.csv("abfss://demo@mathiasf1datalake.dfs.core.windows.net/circuits.csv"))

#Read data from circuits.csv avec 
# protocole mount -> écriture sémantique
display(spark.read.csv("/mnt/mathiasf1datalake/demo/circuits.csv"))

# COMMAND ----------

# Partie 4 Lister les mounts
# Le 6ème est le mien --> on voit sur quel contaier et storage-account il pointe
display(dbutils.fs.mounts())

#Rretirer un mount de son choix
# display(dbutils.fs.unmount('/mnt/mathiasf1datalake/demo'))

# COMMAND ----------

