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
# MAGIC **Left Outer Join** \
# MAGIC Join on circuit.id

# COMMAND ----------

# Left Outer Join
race_left_circuits_df = circuits_filtered_df.join(races_filtered_df, circuits_filtered_df.circuit_id == races_filtered_df.circuit_id, "left") \
.select(circuits_filtered_df.circuit_name, circuits_filtered_df.location, circuits_filtered_df.country, races_filtered_df.race_name, races_filtered_df.round) 

display(race_left_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Right Outer Join** \
# MAGIC Join on circuit.id

# COMMAND ----------

# Right Outer Join
race_right_circuits_df = circuits_filtered_df.join(races_filtered_df, circuits_filtered_df.circuit_id == races_filtered_df.circuit_id, "right") \
.select(circuits_filtered_df.circuit_name, circuits_filtered_df.location, circuits_filtered_df.country, races_filtered_df.race_name, races_filtered_df.round) 

display(race_right_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Full Outer Join** \
# MAGIC Join on circuit.id

# COMMAND ----------

# Full Outer Join
race_full_circuits_df = circuits_filtered_df.join(races_filtered_df, circuits_filtered_df.circuit_id == races_filtered_df.circuit_id, "full") \
.select(circuits_filtered_df.circuit_name, circuits_filtered_df.location, circuits_filtered_df.country, races_filtered_df.race_name, races_filtered_df.round) 

display(race_full_circuits_df)
