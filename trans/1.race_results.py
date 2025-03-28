# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Lectures des deltaTables https://docs.delta.io/latest/delta-batch.html#read-a-table

# COMMAND ----------

# Lecture de la table par son chemin
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

consructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") \
.withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("ingestion_date", "race_date")

# COMMAND ----------

# Placement d'un filtre pour faire le reprocessing sur la date ajoutée seulement
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "results_race_id") \
    .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join circuits to races

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join results to all dataframes

# COMMAND ----------

races_results_df = results_df.join(races_circuits_df, results_df.results_race_id == races_circuits_df.race_id, "inner") \
    .join(consructors_df, consructors_df.constructor_id == results_df.constructor_id) \
    .join(drivers_df, drivers_df.driver_id == results_df.driver_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = races_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# MAGIC %md Stockage format parquet

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

# MAGIC %md Stockage format delta

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id 
# MAGIC from f1_presentation.race_results 
# MAGIC group by race_id 
# MAGIC order by race_id desc
# MAGIC limit 10;