# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

#SQL syntaxe
races_filtered_df_SQL = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

# PYTHON Syntaxe
races_filtered_df_Python = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df_Python)

# COMMAND ----------

