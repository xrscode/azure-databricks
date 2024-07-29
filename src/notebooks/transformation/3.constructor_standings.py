# Databricks notebook source
# MAGIC %md
# MAGIC **Constructor Standings**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

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


# COMMAND ----------

# MAGIC %md
# MAGIC **Read data into dataframe**

# COMMAND ----------

race_results_df = spark.read.parquet(presentation_races)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc
constructor_standings_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins")) 

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(presentation_constructor_standings)

# COMMAND ----------

try:
    final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.constructor_standings")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(presentation_constructor_standings):
            dbutils.fs.rm(presentation_constructor_standings, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.constructor_standings")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/constructor_standings"))
