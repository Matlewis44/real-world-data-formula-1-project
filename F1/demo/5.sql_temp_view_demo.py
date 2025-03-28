# Databricks notebook source
# Ne fonctionne pas car 'Vue temporeaire local'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

# Fonctionne pas car 'Vue temporeaire global'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM global_temp.gv_race_results;

# COMMAND ----------

