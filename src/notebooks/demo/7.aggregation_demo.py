# Databricks notebook source
# MAGIC %md
# MAGIC **Aggregate functions demo**

# COMMAND ----------

# MAGIC %md
# MAGIC Built in Aggregate functions

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

# MAGIC %md
# MAGIC Create smaller dataframe to work with:

# COMMAND ----------

demo_df = race_results_df.filter("race_year=2020")

# COMMAND ----------

# MAGIC %md
# MAGIC Import aggregate functions

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

# MAGIC %md
# MAGIC **Count and countDistinct**

# COMMAND ----------

# Counts the number of race_name's.  Creates a table (.show()):
demo_df.select(count("race_name")).show()

# Counts the number of distinct race_name's.  Creates a table (.show()):
demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Sum**

# COMMAND ----------

demo_df.filter("driver_name = 'Max Verstappen'").select(sum("points")).show()
demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points").withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Group By Aggregations**

# COMMAND ----------

from pyspark.sql.functions import desc

demo_df.groupBy("driver_name") \
  .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_races")) \
  .orderBy(desc("total_points")) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Window Function**

# COMMAND ----------

# Creates a dataframe with race years from 2019 and 2020.
demo_df = race_results_df.filter("race_year in (2019, 2020)")

demo_grouped_df = demo_df.groupBy("race_year", "driver_name") \
  .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_races"))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df = demo_grouped_df.withColumn("rank", rank().over(driverRankSpec))
display(demo_grouped_df)
