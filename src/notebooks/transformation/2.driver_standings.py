# Databricks notebook source
# MAGIC %md
# MAGIC **Produce Driver Standings**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

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
presentation_circuits = f"{mount_dict['presentation']}/circuits"
presentation_races = f"{mount_dict['presentation']}/races"
presentation_constructors = f"{mount_dict['presentation']}/constructors"
presentation_results = f"{mount_dict['presentation']}/race_results"
presentation_drivers = f"{mount_dict['presentation']}/drivers"   
presentation_driver_standings = presentation_drivers = f"{mount_dict['presentation']}/driver_standings"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data into dataframe**

# COMMAND ----------

race_results_df = spark.read.parquet(presentation_results)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(presentation_driver_standings)

# COMMAND ----------

try:
    final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.driver_standings")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(presentation_driver_standings):
            dbutils.fs.rm(presentation_driver_standings, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.driver_standings")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/driver_standings"))
