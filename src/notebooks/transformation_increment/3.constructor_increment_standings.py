# Databricks notebook source
# MAGIC %md
# MAGIC **Constructor Standings**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Establish file paths**

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

# Presentation folder paths
presentation_races = f"{mount_dict['presentation']}/race_results"
presentation_constructor_standings = f"{mount_dict['presentation']}/constructor_standings"


database = "f1_presentation"
table_name = "constructor_standings"
partition = "race_year"


# COMMAND ----------

# MAGIC %md
# MAGIC **Read data into dataframe**

# COMMAND ----------

race_results_df = spark.read.parquet(presentation_races) \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc

race_results_df = spark.read.parquet(presentation_races) \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

overwrite_partition(final_df, database, table_name, partition)
