# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce constructors standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_years_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ancienne Version

# COMMAND ----------

# race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
#   .filter(f"file_date = '{v_file_date}'") \
#   .select("race_year") \
#   .distinct() \
#   .collect()

# COMMAND ----------

# race_years_list = []
# for i in race_results_list :
#   race_years_list.append(i.race_year)

# print(race_years_list)

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_years_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
.groupBy("race_year","team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# MAGIC %md
# MAGIC Window explication
# MAGIC
# MAGIC   fonction Window -> permet de définir une fenêtre de partition pour des calculs analytiques ultérieurs, 
# MAGIC  en particulier pour classer les données en fonction des colonnes 

# COMMAND ----------

# Le rang est ordonné par les victoires pour chaque roupe d'années
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
#Ajout colonne "rank" - calculer le classement des lignes en fonction des critères spécifiés dans driver_rank_spec
final_df = constructor_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ancienne version : Format parquet

# COMMAND ----------

# v1
# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructors_standings")

# v2
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructors_standings")

# overwrite_partition(final_df, 'f1_presentation', 'constructors_standings  ', 'race_year')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Nouvelle version : Format delta

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructors_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructors_standings WHERE race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.constructors_standings
# MAGIC  GROUP BY race_year
# MAGIC  ORDER BY race_year DESC;

# COMMAND ----------

