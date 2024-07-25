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

notebook_dicts = [{"file": "1.ingest_circuits_csv", "time": 0, "data": {"p_data_source": "Ergast API"}},
                  {"file": "2.ingest_races_csv", "time": 0, "data": {"p_data_source": "Ergast API"}},
                  {"file": "3.ingest_constructors_json", "time": 0, "data": {"p_data_source": "Ergast API"}},
                  {"file": "4.ingest_drivers_json", "time": 0, "data": {"p_data_source": "Ergast API"}},
                  {"file": "5.ingest_results_json", "time": 0, "data": {"p_data_source": "Ergast API"}},
                  {"file": "6.ingest_pitstops_json", "time": 0, "data": {"p_data_source": "Ergast API"}},
                  {"file": "7.ingest_lap_times_csv", "time": 0, "data": {"p_data_source": "Ergast API"}},
                  {"file": "8.ingest_qualifying_json", "time": 0, "data": {"p_data_source": "Ergast API"}}
                  ]


for note in notebook_dicts:
    try:
        print(f"Attempting notebook: {note['file']}")
        dbutils.notebook.run(note['file'], note['time'], note['data'])
    except Exception as e:
        print(f"Error! Message: {e}")


# COMMAND ----------


