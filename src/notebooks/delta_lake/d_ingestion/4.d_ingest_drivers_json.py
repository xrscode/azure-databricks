# Databricks notebook source
# MAGIC %md
# MAGIC **Delta Ingest Drivers**

# COMMAND ----------

# March 21st:
results_file_1 = "/mnt/f1dl9072024/raw-increment/2021-03-21/results.json"
# Create temp view so SQL can be performed.
spark.read.json(results_file_1).createOrReplaceTempView("results_cutover")

# March 28th (id: 1052):
results_file_2 = "/mnt/f1dl9072024/raw-increment/2021-03-28/results.json"
# Create temp view so SQL can be performed
spark.read.json(results_file_2).createOrReplaceTempView("results_w1")


# April 18th (id: 1053):
results_file_3 = "/mnt/f1dl9072024/raw-increment/2021-04-18/results.json"
# Create temp view so SQL can be performed
spark.read.json(results_file_3).createOrReplaceTempView("results_w2")

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
raw_increment_drivers = f"{mount_dict['raw_increment']}/{v_file_date}/drivers.json"

# Write:
processed_drivers = f"{mount_dict['processed']}/drivers"

dbs = "f1_processed"
tbl = "races"
clm = "race_id"  

# COMMAND ----------

# MAGIC %md
# MAGIC **READ the JSON file using the spark dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])
drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(raw_increment_drivers)


# COMMAND ----------

# MAGIC %md
# MAGIC **Rename Columns & Add New Columns**
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop URL**

# COMMAND ----------

final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DELTA**

# COMMAND ----------
try: 
    final_df.write.mode("overwrite").format("delta").saveAsTable(f"f1_processed.drivers")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_drivers):
            dbutils.fs.rm(processed_drivers, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.drivers")
        print("Drivers table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")
