# Databricks notebook source
# MAGIC %md
# MAGIC **Produce Driver Standings**

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
presentation_driver_standings = f"{mount_dict['presentation']}/driver_standings"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data into dataframe**

# COMMAND ----------

race_results_list = spark.read.format('delta').load(presentation_results) \
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct() \
.collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)


# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

race_results_df = spark.read.format('delta').load(presentation_results) \
.filter(col("race_year").isin(race_year_list)) 

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"

presentation = f"{mount_dict['presentation']}"
database_name = "f1_presentation"
table_name = "driver_standings"
partition = "race_year"


merge_delta_data(final_df, database_name, table_name, presentation, merge_condition, partition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_presentation.driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC TABLE EXTENDED f1_presentation.race_results
