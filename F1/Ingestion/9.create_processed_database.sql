-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/mathiasf1datalake/processed"  -- Si on ne spécifie pas, la location sera "default location"


-- COMMAND ----------

DESC DATABASE f1_processed;