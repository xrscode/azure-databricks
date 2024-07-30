# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest pitstops.json**

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# Create Widget
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Establish file paths:**
# MAGIC

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

processed_pitstops = f"{mount_dict['processed']}/pit_stops"
raw_pitstops = f"{mount_dict['raw']}/pit_stops.json"

print(processed_pitstops, raw_pitstops)

# COMMAND ----------

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
.json(raw_pitstops)


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

final_df.write.mode("overwrite").parquet(processed_pitstops)

# COMMAND ----------

try: 
    final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_pitstops):
            dbutils.fs.rm(processed_pitstops, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Check Parquet**

# COMMAND ----------

display(spark.read.parquet(processed_pitstops))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
