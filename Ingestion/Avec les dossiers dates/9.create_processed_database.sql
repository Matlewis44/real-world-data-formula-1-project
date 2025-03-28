-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed LOCATION '/mnt/mathiasf1datalake/processed'; -- Si on ne spécifie pas, la location sera "default location"


-- COMMAND ----------

DESC DATABASE EXTENDED f1_processed;