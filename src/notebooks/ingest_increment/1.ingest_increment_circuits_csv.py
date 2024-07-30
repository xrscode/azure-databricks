# Databricks notebook source
# MAGIC %run "../utils/1.prepare_for_incremental_load"

# COMMAND ----------

# MAGIC %md
# MAGIC **Ingest Circuits INCREMENT** \
# MAGIC Widget: "Ergast API"

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

processed_circuits = f"{mount_dict['processed']}/circuits"
raw_increment = f"{mount_dict['raw_increment']}"   

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Widget for Data Source**

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Widget for File Date**

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC **Display Mount Points** \
# MAGIC Locate: /mnt/f1dl9072024/raw-increment
# MAGIC

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC **List files in: /mnt/f1dl9072024/raw-increment** \ 
# MAGIC This command shows all of the files we have access to.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dl9072024/raw-increment

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Specify Schema**

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
.csv(f"{raw_increment}/{v_file_date}/circuits.csv")  

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

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to DataLake as Parquet**

# COMMAND ----------

try:
    final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.circuits")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        # If folder exists at path: /mnt/f1dl9072024/processed/circuits
        if dbutils.fs.ls(processed_circuits):
            # Delete folder:
            dbutils.fs.rm(processed_circuits, True)
        # Re-write Table:
        final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.circuits")
    except Exception as e:
        print(f"Exception occured: {e}")


# COMMAND ----------

# # Read parquet to verify:
display(spark.read.parquet(processed_circuits))
