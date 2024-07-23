# Databricks notebook source
# MAGIC %md
# MAGIC **Filter Transformations**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Data** 

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name")

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("name", "circuit_name") 

# COMMAND ----------

# MAGIC %md
# MAGIC **Filter** \
# MAGIC Make dataframe easier to read.

# COMMAND ----------

# Filter by race year:
races_filtered_df = races_df.filter("race_year = 2019")

# Filter by circuit_id:
circuits_filtered_df = circuits_df.filter("circuit_id < 70")

# COMMAND ----------

# MAGIC %md
# MAGIC **Cross Join**

# COMMAND ----------

race_circuits_df = races_filtered_df.crossJoin(circuits_filtered_df)

display(race_circuits_df)
display(race_circuits_df.count())