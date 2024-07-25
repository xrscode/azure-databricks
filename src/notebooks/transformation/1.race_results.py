# Databricks notebook source
# MAGIC %md
# MAGIC **Race Results**

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Establish mountpoint**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Read Data - Convert to DataFrame** 
# MAGIC 1. Drivers
# MAGIC 2. Constructors 
# MAGIC 3. Circuits 
# MAGIC 4. Races  
# MAGIC 5. Results
# MAGIC
# MAGIC Note: Read from ingested parquet files.
# MAGIC

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Join Circuits to Races**
# MAGIC

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Join results to all other dataframes**

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

# MAGIC %md
# MAGIC **5. Select required columns**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, desc

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position").withColumn("created_date", current_timestamp())

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC **6. Write to Container as Parquet File**

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

end_path = 'race_results'
 
try:
    final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.{end_path}")
    print(f"{end_path.capitalize()} table successfully created.")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        path = f"{presentation_folder_path}/{end_path}"
        if dbutils.fs.ls(path):
            dbutils.fs.rm(path, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.{end_path}")
        print(f"{end_path.capitalize()} table successfully created.")

    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC **7. Check parquet**

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))
