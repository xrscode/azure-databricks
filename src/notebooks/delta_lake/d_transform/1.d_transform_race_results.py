# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")        

# COMMAND ----------

# MAGIC %md
# MAGIC **Race Results**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Data - Convert to DataFrame** 
# MAGIC 1. Drivers
# MAGIC 2. Constructors 
# MAGIC 3. Circuits 
# MAGIC 4. Races  
# MAGIC 5. Results
# MAGIC
# MAGIC Note: Read from ingested parquet files.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Establish File Paths**

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

# Processed folder paths
processed_circuits = f"{mount_dict['processed']}/circuits"
processed_races = f"{mount_dict['processed']}/races"
processed_constructors = f"{mount_dict['processed']}/constructors"
processed_results = f"{mount_dict['processed']}/results"
processed_drivers = f"{mount_dict['processed']}/drivers"

# Presentation folder paths
presentation = f"{mount_dict['presentation']}"
presentation_circuits = f"{mount_dict['presentation']}/circuits"
presentation_races = f"{mount_dict['presentation']}/races"
presentation_constructors = f"{mount_dict['presentation']}/constructors"
presentation_results = f"{mount_dict['presentation']}/race_results"
presentation_drivers = f"{mount_dict['presentation']}/drivers"

database_name = "f1_presentation"
table_name = "race_results"
partition = "race_id"

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Dataframes**

# COMMAND ----------

races_df = spark.read.format('delta').load(processed_races).withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

drivers_df = spark.read.format('delta').load(processed_drivers).withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")

constructors_df = spark.read.format('delta').load(processed_constructors).withColumnRenamed("name", "team")

circuits_df = spark.read.format('delta').load(processed_circuits).withColumnRenamed("location", "circuit_location")

results_df = spark.read.format('delta').load(processed_results) \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Join Circuits to Races**
# MAGIC

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **Join results to all other dataframes**

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

# MAGIC %md
# MAGIC **Select required columns**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, desc

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to Container as Parquet File**

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, database_name, table_name, presentation, merge_condition, partition)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC **Check Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;
