-- Databricks notebook source
DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/mathiasf1datalake/presentation";

-- COMMAND ----------

DESC DATABASE EXTENDED f1_presentation;