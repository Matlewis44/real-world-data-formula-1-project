-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo; 

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo; 

-- COMMAND ----------

desc database extended default;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------



-- COMMAND ----------

-- We switch on demo database
USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table 
-- MAGIC
-- MAGIC ##### Caracteristics
-- MAGIC 1. When we drop a table, it drops both data and metadata fileSystem (folder and files)
-- MAGIC 2. Spark maintains metadata and the data itself

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- Ici on convertit la dataframe en table pour CREER une vrai tablle
-- On spécifie le format et la bdd.nom_de_ma_table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

-- En scrollant en bas dans la table detailed
-- Permet de voir la location, le type (Managed ou External)...
DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2020;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an external table
-- MAGIC
-- MAGIC ##### Caracteristics
-- MAGIC 1. When we drop a table, it drops data in notebook but not metadata fileSystem (folder and files)
-- MAGIC 1. we have to run command to remove it separately
-- MAGIC 3. Spark maintains metadata but we need to manage the data itself

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{demo_folder_path}/race_results")

-- COMMAND ----------

-- On convertit la dataframe en table de la même manière qu'avec la MANAGED Tables
-- La différence est qu'on spécifie le path où la donnée doit s'écrire

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{demo_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/mathiasf1datalake/demo/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW globalv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.globalv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- Création d'une vue permanente enregistré dans Hive Meta Store
-- Accessible en démarrant un nouveau cluster

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results;