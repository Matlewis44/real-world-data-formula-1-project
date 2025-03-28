# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

# dbutils.widgets.remove("donnée_entrante")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read CSV file using dataframeReaderAPIs
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/index.html

# COMMAND ----------

#On cherche notre mountPoint
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md les fichiers contenus dans le blob container raw

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mathiasf1datalake/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# COMMAND ----------

# Définir le schéma manuellement
# Permet de pas utiliser "option("inferSchema", True)" car prend un job spark de plus 
# Il lit toutes les données pour récuperer le type = amoindrit la perf.
circuits_schema = StructType(fields = [
    StructField("circuitId", IntegerType(), False),  # False car ne peut pas être nul
    StructField("circuitRef", StringType(), True),  # Remplacez "col2" et IntegerType() en fonction de votre schéma
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("lng", StringType(), True),
    StructField("alt", StringType(), True),
    StructField("url", StringType(), True),

])

# COMMAND ----------

circuits_df = spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# On vérifie qu'il s'agisse bien d'une DataFrame
type(circuits_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the required columns
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select

# COMMAND ----------

# Voici les 3 manières

# Avec les colonnes en dures
# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt") 

# Avec la variable DataFrame associée
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt) 

# Pareil mais avec [""]
circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], 
                                          circuits_df["name"], circuits_df["location"], circuits_df["country"], 
                                          circuits_df["lat"], circuits_df["lng"], circuits_df["alt"]) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Avec la colonnes 'col'

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# Choix préferentiel du formateur car plus flexible
# Plus court et possibilité de mettre des alias
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name").alias("race_name"), 
                                          col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Rename permanently a column (DataframeWriterAPIs)
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumnRenamed.html#pyspark.sql.DataFrame.withColumnRenamed
# MAGIC

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_Ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_input", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 Add ingestion date to the dataframe
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html#pyspark.sql.DataFrame.withColumn

# COMMAND ----------

# On ajoute simplement une nouvelle colonne
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# Permet d'ajouter un col. et inscrire une value pour toutes les lignes
# withColumn("env", lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as delta
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html#pyspark.sql.DataFrameWriter.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Création de la managed-table circuits dans la bdd
# MAGIC 2. Enregistrement du dossier dans le chemin spécifier à la création de la bdd dans le file"/Formula 1/Ingestion/9.create_processed_database"

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in f1_processed;

# COMMAND ----------

# Ne peut pas se réexecuter 2 fois
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit('success')