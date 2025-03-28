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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
name_schema = StructType([
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ])

# COMMAND ----------

drivers_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])


# COMMAND ----------

drivers_df = spark.read \
.option("header",True) \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted column from the dataframe
# MAGIC https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html#pyspark.sql.DataFrame.drop

# COMMAND ----------

from pyspark.sql.functions import col

drivers_dropped_df = drivers_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

drivers_df_with_timestamp = add_ingestion_date(drivers_dropped_df);

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

drivers_renamed_df = drivers_df_with_timestamp.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumnRenamed("dob", "driver_brithdate") \
    .withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname"))) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to processed container in parquet format

# COMMAND ----------

# drivers_df_with_timestamp.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
# drivers_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")
drivers_renamed_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers
# MAGIC order by driver_brithdate desc;

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/drivers"))
# display(spark.read.parquet(f"{processed_folder_path}/drivers_v2"))

# COMMAND ----------

dbutils.notebook.exit('success')