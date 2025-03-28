# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake containers for the project
# MAGIC #### Steps to follow
# MAGIC  1. Get Client_id, Tenant_Id & client_secret from key Vault
# MAGIC  2. Set Spark Config with App/Client id, Directory/Tenant Id & Secret
# MAGIC  3. Call file system utility mount to mount the storage
# MAGIC  4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Get secrets from Key Vault
    client_ID = service_credential = dbutils.secrets.get(scope="formula1-scope",key="formula1-client-ID")
    tenant_ID = dbutils.secrets.get(scope="formula1-scope",key="formula1-tenant-Id")
    client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secret")
    
    #Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_ID,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_ID}/oauth2/token"}
    
    #Permet la réexecution perpetuelle de la commande --> supprime le mountPoint s'il existe
    # if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
    #     dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    #Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}", #sur quel container et storage-account on va pointer
        extra_configs = configs)
    #Equivalent du print()
    display(dbutils.fs.mounts()) 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ligne Mount container

# COMMAND ----------

mount_adls('mathiasf1datalake', 'raw')

# COMMAND ----------

mount_adls('mathiasf1datalake', 'presentation')

# COMMAND ----------

mount_adls('mathiasf1datalake', 'processed')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mathiasf1datalake/demo

# COMMAND ----------

