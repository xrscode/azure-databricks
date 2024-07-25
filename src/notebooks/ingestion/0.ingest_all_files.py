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

notebook_list = ["1.ingest_circuits_csv", 0, {"p_data_source": "Ergast API"}, 
                 "2.ingest_races_csv", 0, {"p_data_source": "Ergast API"},
                 "3.ingest_constructors_json", 0, {"p_data_source": "Ergast API"},
                 "4.ingest_drivers_json", 0, {"p_data_source": "Ergast API"},
                 "5.ingest_results_json", 0, {"p_data_source": "Ergast API"},
                 "6.ingest_pitstops_json", 0, {"p_data_source": "Ergast API"},
                 "7.ingest_lap_times_csv", 0, {"p_data_source": "Ergast API"},
                 "8.ingest_qualifying_json", 0, {"p_data_source": "Ergast API"}] 


for notebook in notebook_list:
    try:
        notebook
    except Exception as e:
        print(f"Error! Message: {e}")

