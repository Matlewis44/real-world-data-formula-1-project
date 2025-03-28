# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folder in the dbfs root
# MAGIC 2. Interact with dbfs file browser
# MAGIC 3. Upload file with DBFS Root
# MAGIC
# MAGIC Permet de lire des données sans authentification requise

# COMMAND ----------

# Clic droit sur le compte --> param. admin
# --> workspace settings --> filter search dbfs --> Enable DBFS 
# --> données (data) --> Parcourir DBFS --> importer un file au choix

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))