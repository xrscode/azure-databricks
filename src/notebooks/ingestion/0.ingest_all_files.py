# Databricks notebook source
# MAGIC %md
# MAGIC **Create Tables Ingest Files**

# COMMAND ----------

# MAGIC %md
# MAGIC **Create tables First:**

# COMMAND ----------

# MAGIC %run "./9.create_processed_database"

# COMMAND ----------

# MAGIC %run "../raw/1.create_raw_tables"

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC **Run remaining notebooks** \
# MAGIC Remaining notebooks will populate the previously created tables.

# COMMAND ----------

notebooks = [
dbutils.notebook.run("1.ingest_circuits_csv", 0, {"p_data_source": "Ergast API"}),
dbutils.notebook.run("2.ingest_races_csv", 0, {"p_data_source": "Ergast API"}),
dbutils.notebook.run("3.ingest_constructors_csv", 0, {"p_data_source": "Ergast API"}),
dbutils.notebook.run("4.ingest_drivers_json", 0, {"p_data_source": "Ergast API"}),
dbutils.notebook.run("5.ingest_results_json", 0, {"p_data_source": "Ergast API"}),
dbutils.notebook.run("6.ingest_pitstops_json", 0, {"p_data_source": "Ergast API"}),
dbutils.notebook.run("7.ingest_lap_times_csv", 0, {"p_data_source": "Ergast API"}),
dbutils.notebook.run("8.ingest_qualifying_json", 0, {"p_data_source": "Ergast API"}),
]

for i, notebook in enumerate(notebooks):
    v_result = notebook
    if v_result == "Success":
        print(f"Notebook number: {i+1} success.  Continuing...")
    else:
        print(f"Error on notebook number: {i+1}")

