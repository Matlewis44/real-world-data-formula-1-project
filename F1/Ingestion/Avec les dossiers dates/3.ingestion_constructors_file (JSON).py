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

#On cherche notre mountPoint
# display(dbutils.fs.mounts())

#On verifie que les données ont bien été ingéré

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mathiasf1datalake/raw/2021-03-21

# COMMAND ----------


# Utile pour connaitre les types des colonnes
# constructors_df.printSchema()
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.option("header",True) \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted column from the dataframe
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html#pyspark.sql.DataFrame.drop

# COMMAND ----------

from pyspark.sql.functions import col
# Les 3 manières ci-dessous

constructors_dropped_df = constructors_df.drop('url')
# constructors_dropped_df = constructors_df.drop(constructors_df['url'])
# constructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructors_df_with_timestamp = add_ingestion_date(constructors_dropped_df);

# COMMAND ----------

from pyspark.sql.functions import lit
constructors_final_df = constructors_df_with_timestamp.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

# constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
# constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# Technique avec PySpark même chose qu'avec sql
# display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit('success')