# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest Increment All Files**

# COMMAND ----------

# MAGIC %run "../utils/1.prepare_for_incremental_load"

# COMMAND ----------

# MAGIC %md
# MAGIC **List of Files**

# COMMAND ----------

file_list = ["1.ingest_increment_circuits_csv", 
             "2.ingest_increment_races_csv",
             "3.ingest_increment_constructors_json",
             "4.ingest_increment_drivers_json",
             "5.ingest_increment_results_json",
             "6.ingest_increment_pitstops_json",
             "7.ingest_increment_lap_times_csv",
             "8.ingest_increment_qualifying_json"]

# COMMAND ----------

# MAGIC %md
# MAGIC **List of Dictionaries**

# COMMAND ----------

dict_list = [
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
    {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"},
    {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"}
]

# COMMAND ----------

# MAGIC %md
# MAGIC **Iterate through file_list**

# COMMAND ----------

for file in file_list:
  for dict in dict_list:
    try:
      dbutils.notebook.run(file, 0, dict)
    except Exception as e:
      print('Error message: {e}')
