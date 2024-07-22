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

races_df_filtered = races_df.filter("race_year = 2019")
# display(races_df_filtered)
# Now there are only 19 records.

# COMMAND ----------

# MAGIC %md
# MAGIC **Join** \
# MAGIC Join on circuit.id

# COMMAND ----------

# Python syntax:
race_circuits_df = circuits_df.join(races_df_filtered, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df_filtered.race_name , races_df_filtered.round)
display(race_circuits_df)
