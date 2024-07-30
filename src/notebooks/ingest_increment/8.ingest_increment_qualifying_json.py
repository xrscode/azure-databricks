# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest Increment Qualifying**

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Widget for Data Source**
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Widget for File Date**

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Load config and common functions**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Establish File Paths:**
# MAGIC

# COMMAND ----------

import json
# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

# Read:
raw_increment_qualifying = f"{mount_dict['raw_increment']}/{v_file_date}/qualifying"

dbs = "f1_processed"
tbl = "qualifying"
clm = "race_id"  


# COMMAND ----------

# MAGIC %md
# MAGIC **Define Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC **Ingest Multiple JSON's**

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiLine', True).json(raw_increment_qualifying)

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename Columns and Add**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualifying_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to Parquet**

# COMMAND ----------

overwrite_partition(final_df, dbs, tbl, clm)
