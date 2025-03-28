# Databricks notebook source
# Cas normal
raw_folder_path = '/mnt/mathiasf1datalake/raw'
processed_folder_path = '/mnt/mathiasf1datalake/processed'
presentation_folder_path = '/mnt/mathiasf1datalake/presentation'
demo_folder_path = '/mnt/mathiasf1datalake/demo'

# COMMAND ----------

# Cas où pas accès au mount (Entreprise ou Student Subscription)

# raw_folder_path = 'abfss://raw@mathiasf1datalake.dfs.core.windows.net'
# processed_folder_path = 'abfss://processed@mathiasf1datalake.dfs.core.windows.net'
# presentation_folder_path = 'abfss://presentation@mathiasf1datalake.dfs.core.windows.net'