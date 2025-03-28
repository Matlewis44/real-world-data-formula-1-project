# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### INCREMENTAL_LOAD

# COMMAND ----------

# Commande pour placer la colonne en paramètre à la fin
def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

#### 'spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")' 
# Permet de récuperer les partitions concérnés uniquement quand 'insertInto' s'éxecute
# pour remplacer ces partitions avec les nouvelles données
# Le statique écrase toute la table

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        # On ne peut spécifier l'ordre des colonnes insérées
        # Partitionnement automatique sur la dernière colonne (sparks)
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# input_df : Un DataFrame contenant les données à traiter.
# column_name : Le nom de la colonne à convertir en liste.
# La fonction renvoie une liste de valeurs distinctes de la colonne spécifiée.
def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
# collect() : convertit la liste df_row_list en une liste Python standard.
#  extrait les valeurs de la colonne spécifiée de chaque ligne du DataFrame et les stocke dans la liste
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

# MAGIC %md
# MAGIC - Fonction de merge en PySpark (syntaxe plus complexe mais plus performant car données persistées en binaire dans deltaTable vs format texte de SQL) le moteur de bdd gérant le deltaTable n'a pas à convertit les données en binaire
# MAGIC - 
# MAGIC - Always specify partition colunms in merge condition, so that the query will be quicker
# MAGIC (bonne pratique pour les grands projets)

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")