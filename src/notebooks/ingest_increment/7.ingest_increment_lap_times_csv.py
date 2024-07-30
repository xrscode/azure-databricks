# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest lap_times.csv**

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
# MAGIC **Establish Paths:**
# MAGIC

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

processed_lap_times = f"{mount_dict['processed']}/lap_times"
raw_lap_times = f"{mount_dict['raw']}/lap_times"

print(processed_lap_times, raw_lap_times)

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
.csv(raw_lap_times)


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
# MAGIC **Write to Parquet**

# COMMAND ----------

final_df.write.mode("overwrite").parquet(processed_lap_times)

# COMMAND ----------

try: 
    final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")
   
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_lap_times):
            dbutils.fs.rm(processed_lap_times, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")
        print("Constructors table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Check Write**

# COMMAND ----------

display(spark.read.parquet(processed_lap_times))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
