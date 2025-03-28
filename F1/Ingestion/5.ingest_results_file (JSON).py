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
results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read \
.option("header",True) \
.schema(results_schema) \
.json(f'{raw_folder_path}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted column and rename some from the dataframe
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html#pyspark.sql.DataFrame.drop

# COMMAND ----------

from pyspark.sql.functions import col

results_dropped_df = results_df.drop(col('statusId'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit
    # On met toujours le current_timestamp() pour dater l'ingestion

# Ajouter la colonne de date d'ingestion
results_df_with_timestamp = add_ingestion_date(results_dropped_df)

# Renommer les colonnes dans le DataFrame résultant
results_renamed_df = results_df_with_timestamp \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

# results_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")
results_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results_v2")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))
display(spark.read.parquet(f"{processed_folder_path}/results_v2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Même chose qu'au-dessus
# MAGIC select * from f1_processed.constructors_v2;

# COMMAND ----------

dbutils.notebook.exit('success')