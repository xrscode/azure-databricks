# Databricks notebook source
# MAGIC %md
# MAGIC **Access dataframes using SQL**
# MAGIC This notebook will demonstrate that local notebooks can't be accessed.
# MAGIC Global notebooks CAN be accessed from separate notebooks. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;
