# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest Increment Pitstops**

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
raw_increment_pit_stops = f"{mount_dict['raw_increment']}/{v_file_date}/pit_stops.json"

# Write:
processed_pitstops = f"{mount_dict['processed']}/pit_stops"

dbs = "f1_processed"
tbl = "pit_stops"
clm = "race_id"  

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC **Define Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
 
pit_schema = StructType(fields=[ 
StructField("raceId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("stop", IntegerType(), True),
StructField("time", StringType(), True),
StructField("duration", StringType(), True),
StructField("milliseconds", IntegerType(), True)
]) 

# COMMAND ----------

# MAGIC %md
# MAGIC **Read multi-line JSON**

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_schema) \
.option("multiLine", True) \
.json(raw_increment_pit_stops)


# COMMAND ----------

# MAGIC %md
# MAGIC **Rename & Add Ingestion Date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = pit_stops_df \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Parquet**

# COMMAND ----------

overwrite_partition(final_df, dbs, tbl, clm)


# COMMAND ----------

# MAGIC %md
# MAGIC **Check Parquet**

# COMMAND ----------

display(spark.read.parquet(processed_pitstops))

# COMMAND ----------

# MAGIC %md
# MAGIC
