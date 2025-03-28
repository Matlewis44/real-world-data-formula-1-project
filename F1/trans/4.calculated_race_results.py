# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ancienne version : Format parquet
# MAGIC - Pas de charge incrémentale
# MAGIC - Pas de réception du file_date / pas de filtre --> on reprocess toutes les data à chaque action
# MAGIC - Impossible de merger/upadater car problème de création de table à chaque sélection de la data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On affiche les mecs ayant fini top 10
# MAGIC -- comme les points max diffèrent selon les saisons
# MAGIC -- On crée colonne "points calculé" qui va de 10 à 1 pour les top ten
# MAGIC -- On nomme toujours la bdd pour la clarté des requêtes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table f1_presentation.calculated_race_results
# MAGIC -- using parquet
# MAGIC -- AS
# MAGIC -- select races_v2.race_year,
# MAGIC --       constructors_v2.name as team_name,
# MAGIC --       drivers_v2.name AS driver_name,
# MAGIC --       results_v2.position,
# MAGIC --       results_v2.points,
# MAGIC --       11 - results_v2.position as calculated_points
# MAGIC --   from results_v2
# MAGIC --   join f1_processed.drivers_v2 on (drivers_v2.driver_id = results_v2.driver_id)
# MAGIC --   join f1_processed.constructors_v2 on (results_v2.constructor_id = constructors_v2.constructor_id)
# MAGIC --   join f1_processed.races_v2 on (races_v2.race_id = results_v2.race_id)
# MAGIC --   where results_v2.position <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nouvelle version : Format delta
# MAGIC - Charge incrémentale
# MAGIC - Réception du file_date / pas de filtre --> on reprocess juste la data dont on a besoin

# COMMAND ----------

# Création de la table au préalable
# Primary key on race_id et driver_id
# Target table
spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Résultats
# MAGIC - On affiche les mecs ayant fini top 10
# MAGIC - comme les points max diffèrent selon les saisons
# MAGIC - On crée colonne "points calculé" qui va de 10 à 1 pour les top ten
# MAGIC - On nomme toujours la bdd pour la clarté des requêtes

# COMMAND ----------

# Création vue tempo. à partir des résultats avec la data fournit dans file_date
# Cette étape simplife la requête de MERGE
# Permet rassembler la donnée dans une seule table pour le MERGE après

# Source table
spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructors.name AS team_name,
                     drivers.driver_id,
                     drivers.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM f1_processed.results 
                JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
                JOIN f1_processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
                 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MERGE depuis la table vue tempo. en source  
spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On fait le compte sur la vue temporaire
# MAGIC -- 10035 --> 10 --> 10
# MAGIC SELECT COUNT(1) FROM race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On fait le compte sur la table finale
# MAGIC -- 10035 + 10 + 10 = 10055
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from f1_presentation.calculated_race_results
# MAGIC -- order by race_year desc
# MAGIC SELECT DISTINCT race_id
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC order by race_id desc
# MAGIC limit 10;

# COMMAND ----------

