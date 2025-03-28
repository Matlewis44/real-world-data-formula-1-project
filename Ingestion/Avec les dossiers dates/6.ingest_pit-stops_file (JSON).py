# Databricks notebook source
# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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
.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename  column from the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit
    # On met toujours le current_timestamp() pour dater l'ingestion
# Ajouter la colonne de date d'ingestion
pit_stops_df_with_timestamp = add_ingestion_date(pit_stops_df)

pit_stops_final_df = pit_stops_df_with_timestamp \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# les colonnes sont renommée mais pas persistées en tant que tel
display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

# pit_stops_df_with_timestamp.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# pit_stops_df_with_timestamp.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC #### New Step 3 - Write output to processed container in delta format

# COMMAND ----------

# Spécifie la partition avec la codition de merge pour gain en temps et perf. lors de l'execution

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.pit_stops 
# MAGIC  group by race_id
# MAGIC  order by race_id desc
# MAGIC limit 10;

# COMMAND ----------

dbutils.notebook.exit('success')