# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read CSV file using dataframeReader
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/index.html

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mathiasf1datalake/processed/circuits

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mathiasf1datalake/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# https://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.sql.html#data-types

# COMMAND ----------

races_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),  # False car ne peut pas être nul
    StructField("year", IntegerType(), True),  
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header",True) \
.schema(races_schema) \
.csv(f'{raw_folder_path}/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the required columns
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Avec la colonnes 'col'

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Choix préferentiel du formateur car plus flexible
# Avec la colonnes 'col'
races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), 
                                          col("name").alias("race_name"), col("date"), 
                                          col("time"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Rename permanently a column
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumnRenamed.html#pyspark.sql.DataFrame.withColumnRenamed
# MAGIC

# COMMAND ----------

# Ne sert à rien si on utilise l'alias dans le select
races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")


# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 Add ingestion date to the dataframe
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html#pyspark.sql.DataFrame.withColumn

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

# Supposez que circuits_selected_df est votre DataFrame avec les colonnes appropriées
races_final_df = add_ingestion_date(races_renamed_df); \
races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(''), col("time")), "yyyy-MM-ddHH:mm:ss"))
    

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html#pyspark.sql.DataFrameWriter.parquet

# COMMAND ----------

# On crée un fichier
# "overwrite" permet de réexecuter la commande sans l'erreur "Already exist"

# Pour un analyse spécifique pour l'année -> Permet d'aller bcp plus vite
# Crée un dossier pour chaque année 
# -> Utile Si on cherche des années spécifiques -> Aide Spark à traiter la data en parallèle

# /!\ Si trop de partitions, alors surcharge du travail Spark

# races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races_v2")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mathiasf1datalake/processed/races_v2

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races_v2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races_v2;

# COMMAND ----------

dbutils.notebook.exit('success')