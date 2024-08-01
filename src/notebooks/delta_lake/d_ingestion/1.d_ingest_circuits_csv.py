# Databricks notebook source
# MAGIC %md
# MAGIC **Delta Ingest Circuits**

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
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

processed_results = f"{mount_dict['processed']}/results"
raw_increment_circuits = f"{mount_dict['raw_increment']}/{v_file_date}/circuits.csv"
processed_circuits = f"{mount_dict['processed']}/circuits"

dbs = "f1_processed"
tbl = "circuits"
clm = "race_id"   

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Dataframe**

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(raw_increment_circuits)  

# COMMAND ----------

# MAGIC %md
# MAGIC **Select columns**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename Columns**

# COMMAND ----------


from pyspark.sql.functions import lit
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DELTA**

# COMMAND ----------
try:
    final_df.write.mode("overwrite").format("delta").saveAsTable(f"{dbs}.{tbl}")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_circuits):
            # Delete folder:
            dbutils.fs.rm(processed_circuits, True)
        # Re-write Table:
        final_df.write.mode("overwrite").format("delta").saveAsTable(f"{dbs}.{tbl}")
        print("Circuits table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_processed.circuits
