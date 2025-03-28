-- Databricks notebook source
-- Supression de toutes les tables contenues dans la bdd
-- Suppression de la bdd ensuite
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/mathiasf1datalake/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/mathiasf1datalake/presentation";