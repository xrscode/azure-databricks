# Databricks notebook source
# MAGIC %md
# MAGIC **Delta Lake Demo**
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (file)

# COMMAND ----------

import json

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

results_location = f"{mount_dict['raw_increment']}/2021-03-28/results.json"
write_demo = f"{mount_dict['demo']}"
managed_table = "/mnt/f1dl9072024/raw-increment/results_managed"


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/f1dl9072024/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json(results_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data to delta lake - managed table**

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data to delta lake - external table**

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save(f"{write_demo}/results_external")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create external table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/f1dl9072024/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/f1dl9072024/demo/results_external")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC **Updates and Delete**
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Update Delta Table using SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Update Delta Table using Python:

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, managed_table)

deltaTable.update("position <= 10", {"points": "21 - position"})


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC **DELETE Delta Lake**

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Delete:

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %md
# MAGIC PYTHON Delete:

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, managed_table)

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed
