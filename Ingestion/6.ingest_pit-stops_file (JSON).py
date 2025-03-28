# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using dataframeReader
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/index.html

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Schéma pour les données JSON
pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# /!\ Activer impérativement l'option de lecture "multiLine"
# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameReader.json.html#pyspark.sql.DataFrameReader.json
pit_stops_df = spark.read \
.option("multiLine",True) \
.schema(pit_stops_schema) \
.json(f'{raw_folder_path}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename  column from the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit
    # On met toujours le current_timestamp() pour dater l'ingestion

pit_stops_df_with_timestamp = add_ingestion_date(pit_stops_df); \
    pit_stops_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") 

# COMMAND ----------

display(pit_stops_df_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

# pit_stops_df_with_timestamp.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
pit_stops_df_with_timestamp.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops_v2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops_v2;

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))
display(spark.read.parquet(f"{processed_folder_path}/pit_stops_v2"))

# COMMAND ----------

dbutils.notebook.exit('success')