# Databricks notebook source
# MAGIC %md
# MAGIC **Create Variables** \
# MAGIC These variables are intended for use in other notebooks.

# COMMAND ----------

project_path = "dbfs:/mnt/f1dl9072024/"
raw_folder_path = f"{project_path}raw"
processed_folder_path = f"{project_path}processed"
presentation_folder_path = f"{project_path}presentation"

# COMMAND ----------

# MAGIC %md
# MAGIC **If Using ABFSS protocol:**

# COMMAND ----------

# raw_folder_path = 'abfss://raw@f1dl9072024.dfs.core.windows.net'
# processed_folder_path = 'abfss://processed@f1dl9072024.dfs.core.windows.net'
# presentation_folder_path = 'abfss://presentation@f1dl9072024.dfs.core.windows.net'
