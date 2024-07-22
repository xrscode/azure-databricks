# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest All Files**

# COMMAND ----------

dbutils.notebook.help()

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

# # COMMAND ----------

# # MAGIC %run "../includes/configuration"

# # COMMAND ----------

# # MAGIC %run "../includes/common_functions"

# # COMMAND ----------

# v_result = dbutils.notebook.run("1.ingest_circuits_csv", 0, {"p_data_source": "Ergast API"})

# # COMMAND ----------

# v_result

# # COMMAND ----------

# v_result = dbutils.notebook.run("2.ingest_races_csv", 0, {"p_data_source": "Ergast API"})

# # COMMAND ----------

# v_result

# # COMMAND ----------

# v_result = dbutils.notebook.run("3.ingest_constructors_csv", 0, {"p_data_source": "Ergast API"})

# # COMMAND ----------

# v_result

# # COMMAND ----------

# v_result = dbutils.notebook.run("4.ingest_drivers_json", 0, {"p_data_source": "Ergast API"})

# # COMMAND ----------

# v_result

# # COMMAND ----------

# v_result = dbutils.notebook.run("5.ingest_results_json", 0, {"p_data_source": "Ergast API"})

# # COMMAND ----------

# v_result

# # COMMAND ----------

# v_result = dbutils.notebook.run("6.ingest_pitstops_json", 0, {"p_data_source": "Ergast API"})

# # COMMAND ----------

# v_result

# # COMMAND ----------

# v_result = dbutils.notebook.run("7.ingest_lap_times_csv", 0, {"p_data_source": "Ergast API"})

# # COMMAND ----------

# v_result

# # COMMAND ----------

# v_result = dbutils.notebook.run("8.ingest_qualifying_json", 0, {"p_data_source": "Ergast API"})

