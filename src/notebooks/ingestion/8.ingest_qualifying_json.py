# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest qualifying.json**

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
# MAGIC **Establish paths:**
# MAGIC

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

processed_qualifying = f"{mount_dict['processed']}/qualifying"
raw_qualifying = f"{mount_dict['raw']}/qualifying"

print(processed_qualifying, raw_qualifying)

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

qualifying_df = spark.read.schema(qualifying_schema).option('multiLine', True).json(raw_qualifying)

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

final_df.write.mode("overwrite").parquet(processed_qualifying)

# COMMAND ----------

try: 
    final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_qualifying):
            dbutils.fs.rm(processed_qualifying, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Check Parquet**

# COMMAND ----------

display(spark.read.parquet(processed_qualifying))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
