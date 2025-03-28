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
lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f'{raw_folder_path}/lap_times')
# wilcard path
# .csv('/mnt/mathiasf1datalake/raw/lap_times/lap_times_split*.csv') Au cas où l'on veut spécifier certains fichiers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename  column from the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

lap_times_final_df = add_ingestion_date(lap_times_df); \
    lap_times_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times_v2")

# COMMAND ----------

dbutils.notebook.exit('success')