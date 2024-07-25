# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest ALL files and create Tables**

# COMMAND ----------

# MAGIC %md
# MAGIC **Create tables First:**

# COMMAND ----------

# MAGIC %run "./9.create_processed_database"

# COMMAND ----------

# MAGIC %run "../raw/1.create_raw_tables"

# COMMAND ----------

# dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC **Mount All Folders**

# COMMAND ----------

# MAGIC %run "../set-up/8.mount_adls_containers_for_project"

# COMMAND ----------

# MAGIC %md
# MAGIC **Run remaining notebooks** \
# MAGIC Remaining notebooks will populate the previously created tables.

# COMMAND ----------

try:   
    dbutils.notebook.run("1.ingest_circuits_csv", 0, {"p_data_source": "Ergast API"})
except Exception as e:
    print(e)

