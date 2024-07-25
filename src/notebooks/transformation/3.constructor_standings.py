# Databricks notebook source
# MAGIC %md
# MAGIC **Constructor Standings**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data into dataframe**

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

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

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

end_path = 'constructor_standings'
 
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

display(spark.read.parquet(f"{presentation_folder_path}/driver_standings"))
