# Databricks notebook source
# MAGIC %md
# MAGIC **Delta Ingest Lap Times**

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
raw_increment_lap_times = f"{mount_dict['raw_increment']}/{v_file_date}/lap_times"
processed_lap_times = f"{mount_dict['processed']}/lap_times"

# COMMAND ----------

# MAGIC %md
# MAGIC **Configure Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Data Frame**

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(raw_increment_lap_times)


# COMMAND ----------

# MAGIC %md
# MAGIC **Rename and Add Column**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumn("ingestion_date", current_timestamp()) \
  .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC **Write to DELTA**

# COMMAND ----------
dbs = "f1_processed"
tbl = "lap_times"
clm = "race_id"  

merge = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df, dbs, tbl, processed_lap_times, merge, clm)
