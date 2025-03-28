# Databricks notebook source
# MAGIC %md
# MAGIC Ici, on ingère : 
# MAGIC - la donnée de toutes les courses de l'histoire jusqu'au 21 mars 2021 (Full load)
# MAGIC - la donnée de la course du 28 Mars 2021 (Incremental load)
# MAGIC - la donnée de la course du 18 Avril 2021 (Incremental load)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists f1_processed.results;

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

# Test d'affichage

# spark.read.json("/mnt/mathiasf1datalake/raw/2021-03-21/results.json").createOrReplaceTempView("result_w2")

# COMMAND ----------

# %sql
# -- Test d'affichage
# SELECT raceId, count(1)
#   FROM result_w2
#   GROUP BY raceId
#   ORDER BY raceId DESC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

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
.json(f'{raw_folder_path}/{v_file_date}/results.json')

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
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the dataframe
# MAGIC - Suppression des data dupliquées

# COMMAND ----------

results_deduped_df = results_renamed_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

# results_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed table in parquet format
# MAGIC ##### overwrite --> Full load

# COMMAND ----------

# results_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sppression de la donnée via la partition (race_Id)
# MAGIC ##### https://spark.apache.org/docs/3.1.1/sql-ref-syntax-ddl-alter-table.html

# COMMAND ----------

# MAGIC %md
# MAGIC ###### La charge incrémentale ajout ajoute les Id sans écraser les précédents
# MAGIC ###### Permet d'effacer la valeur contenu par l'Id et de la remplaccer sans duplication lors d'une réxecution de commande

# COMMAND ----------

# MAGIC %md
# MAGIC ### METHOD 1
# MAGIC ###### Moins efficace car on gère la recherche de partitions et la réécriture manuellement

# COMMAND ----------

# On fait une liste de lignes/colonnes de race_id
# /!\ Ne jamais faire sur un dataFrame normal car trop de ligne (millions)
# Il ramène toute la donnée dans le driver's node memory

# Good pour nous car on fait une agregation avant (distinct)
# On a donc une quantité limité

# We go through the list, alter the table, and drop the partition

# for race_id_list in results_renamed_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processsed.results")):
#         spark.sql(f"ALTER TABLE f1_processsed.results DROP IF EXISTS PARTITION(race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### append --> Incremental load

# COMMAND ----------

# On écrit la data dans notre table
# results_renamed_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### METHOD 2
# MAGIC ###### Plus performant car Spark gère la recherche de partitions et la réécriture automatiquement

# COMMAND ----------

# Permet de récuperer les partitions concérnés uniquement quand 'insertInto' s'éxecute
# pour remplacer ces partitions avec les nouvelles données
# Le statique écrase toute la table

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# Commande pour placer "race_id" à la fin

# results_renamed_df = results_renamed_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order",
                                              #  "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", 
                                              #  "fastest_lap_speed","data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# if(spark._jsparkSession.catalog().tableExists("f1_processsed.results")):
#     # On ne peut spécifier l'ordre des colonnes insérées
#     # Partitionnement automatique sur la dernière colonne (sparks)
#     results_renamed_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_renamed_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Persistance au format parquet

# COMMAND ----------

# overwrite_partition(results_renamed_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC Persistance au format delta

# COMMAND ----------

# Spécifie la partition avec la codition de merge pour gain en temps et perf. lors de l'execution

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On vérifie l'absence de données dupliquées
# MAGIC SELECT race_id, driver_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On récupère les résultats des 20 pilotes du cutover des 1047 courses jusqu'au 2021-03-21
# MAGIC -- On récupère les résultats des 20 pilotes pour la course du 2021-03-28
# MAGIC -- On récupère les résultats des 20 pilotes pour la course du 2021-04-18
# MAGIC
# MAGIC -- Si on recharge la même course; il ajoutera 20 résultat à la ^m course, soit 40 résultats (ACID: Incohérent)
# MAGIC -- Data Lake architecture ne donne pas la capacité de supprimer, updater, merger la donnée correctement ...
# MAGIC
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.results 
# MAGIC  group by race_Id
# MAGIC  order by race_Id desc
# MAGIC limit 10;

# COMMAND ----------

dbutils.notebook.exit('success')