# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/mathiasf1datalake/demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED f1_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/mathiasf1datalake/raw/2021-03-28/results.json")

# COMMAND ----------

# On fait une réecriture dans une nouvelle managed table
# saveAsTable = managed table
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %md
# MAGIC - Read data from delta lake (Table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC - Write data to delta lake (external table)

# COMMAND ----------

# Writing data to a file location rather than managed table
results_df.write.format("delta").mode("overwrite").save("/mnt/mathiasf1datalake/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creation of an external table
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/mathiasf1datalake/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC - Read data from delta lake (File)
# MAGIC - using a reader to read the file directly from external location

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/mathiasf1datalake/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Création d'une table avec les constructorsId en partition

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC - Data lake : Impossible mettre à jour les points dans la même colonne. (Fallait en créer une autre)
# MAGIC - Mise à jour dans la colonne en elle-même grâce à Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version SQL
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# Equivalent en python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/mathiasf1datalake/demo/results_managed")

deltaTable.update("position <= 10", { "points": "21 - position" } ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Suppression en mode sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# Equivalent en python + suppression des NaN
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/mathiasf1datalake/demo/results_managed")

deltaTable.delete("points = 0") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

# DAY 1
# INSERT 10 premiers pilotes
drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/mathiasf1datalake/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

# Update and Insert in one statement
# Uppercase sur ce qu'on a updaté
from pyspark.sql.functions import upper

# DAY 2
drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/mathiasf1datalake/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

# Update les 5 premiers où l'on déjà la data
# Insert les 5 derniers qu'on a pas déjà
from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/mathiasf1datalake/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md Day1
# MAGIC - Possibilité de gérer l'Insert, l'Update et le Delete en même temps
# MAGIC - La clause WHEN MATCHED = on remplace le contenu de la ligne avec l'Id trouvé par les values de table source
# MAGIC - La clause WHEN NOT MATCHED = Id non trouvé -> ajout d'une nouvelle ligne avec les values de table source

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On merge 'drivers_day1' dans la table target 'drivers_merge'
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- OBSERVATION MERGE 10 PREMIER PILOTE | No Updated_Date
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- OBSERVATION MERGE [6 - 10] PILOTE | With Updated_Date
# MAGIC -- OBSERVATION MERGE [11 - 15] PILOTE | No Updated_Date
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3
# MAGIC - Même chose en version Python (PySpark)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/mathiasf1datalake/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OBSERVATION MERGE [1 - 5] PILOTE | With Updated_Date
# MAGIC -- OBSERVATION MERGE [16 - 20] PILOTE | No Updated_Date
# MAGIC -- .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
# MAGIC  SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OBSERVATION Historique globale de la table
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OBSERVATION 2ème version de la table après action
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Même chose mais en prenant le 'TIMESTAMP' qui affichera la table à la version 1
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-12-15T17:51:40.000+0000';

# COMMAND ----------

# version PySpark
df = spark.read.format("delta").option("timestampAsOf", '2023-12-15T17:51:40.000+0000').load("/mnt/mathiasf1datalake/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM Supprime la donnée au bout de 7 jours | pas immédiatement
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pas d'erreur car rien n'a été enlevé
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-12-15T17:51:40.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Suppression immédiate
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On a une erreur ce qui est normal
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2021-06-23T15:40:33.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Suppression accidentelle
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 19 LIGNES
# MAGIC -- SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Un DELETE
# MAGIC -- DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Affichage avant la suppression accidentelle
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On réstaure la donnée en fusionnant la version 3 dans la version actuelle
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Présence de la verion 9 avec le MERGE
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- De nouveau les 20 LIGNES
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs
# MAGIC ###### Chaque action génère un log de transaction que l'on peut lire dans 'results_external/_delta_log/'
# MAGIC ###### Chaque INSERT-UPDATE-DELETE crée un parquet file
# MAGIC ###### SUPPRIMER au bout de 30 jours par Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT les données de la table drivers_merge
# MAGIC -- ONLY WHERE driverId = 1
# MAGIC -- LA PARTITION de la table source est aussi injectée (...3d4f33ea...)
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deuxième INSERT
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création d'un log file /demo/drivers_txn/_delta_log/00000000000000000003.json
# MAGIC -- Création d'une clé 'REMOVE' avec l'id de la partition ajoutée (...3d4f33ea...) en value
# MAGIC DELETE FROM  f1_demo.drivers_txn
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# Création de multiple log transaction *| Creation d'un checkpoint.parquet file toutes les 10 value
# Le checkpoint.parquet a les infos de 0 à 9 - 10 à 19...
# Les log suivants ne se préoccupe uniquement de leurs prédecessuers du dernier checkpoint.parquet mais pas au-delà

# *Requête SQL exécutée à chaque tour de boucle
for driver_id in range(3, 20):
  spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                SELECT * FROM f1_demo.drivers_merge
                WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création d'1 seul log transaction | Requête SQL unique
# MAGIC -- 2 parquet files car il partionne dans 2 fichiers différents {Distribution calcul} (Ex : 19 dans l'un et 1 dans l'autre)
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crétion d'un table au format parquet
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT drivers_merge's data into aur parquet table
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conversion en DELTA
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

# Lecture de la table convertie en Dataframe
df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC Même chose avec syntaxe python

# COMMAND ----------

df.write.format("parquet").save("/mnt/mathiasf1datalake/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/mathiasf1datalake/demo/drivers_convert_to_delta_new`