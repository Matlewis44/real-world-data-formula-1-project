# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using dataframeReader
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/index.html

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Schéma pour les données JSON
qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read \
.option("multiLine",True) \
.schema(qualifying_schema) \
.json(f'{raw_folder_path}/qualifying')
# wilcard path
# .csv('/mnt/mathiasf1datalake/raw/qualifying/qualifying*.csv') Au cas où l'on veut spécifier certains fichiers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename  column from the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

qualifying_final_df = add_ingestion_date(qualifying_df); \
    qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying_v2")

# COMMAND ----------

dbutils.notebook.exit('success')