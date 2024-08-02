# Databricks notebook source
# MAGIC %md
# MAGIC **Delta Ingest Constructors**

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
raw_increment_constructors = f"{mount_dict['raw_increment']}/{v_file_date}/constructors.json"

# Write:
processed_constructors = f"{mount_dict['processed']}/constructors"

dbs = "f1_processed"
tbl = "races"
clm = "race_id"   

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the JSON file using the spark dataframe reader**

# COMMAND ----------

constructors_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(raw_increment_constructors)

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop Unwanted Columns**

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename column and add ingestion_date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DELTA**


# COMMAND ----------
try: 
    constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")
    print("Constructors table successfully created.")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_constructors):
            dbutils.fs.rm(processed_constructors, True)
        constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")
        print("Constructors table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")

