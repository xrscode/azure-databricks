# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest constructors JSON**

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

processed_constructors = f"{mount_dict['processed']}/constructors"
raw_constructors = f"{mount_dict['raw']}/constructors.json"

print(processed_constructors, raw_constructors)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the JSON file using the spark dataframe reader**

# COMMAND ----------

constructors_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(raw_constructors)

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
# MAGIC **Write output to Parquet**

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(processed_constructors)

# COMMAND ----------

try: 
    constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
    print("Constructors table successfully created.")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_constructors):
            dbutils.fs.rm(processed_constructors, True)
        constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
        print("Constructors table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")



# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
