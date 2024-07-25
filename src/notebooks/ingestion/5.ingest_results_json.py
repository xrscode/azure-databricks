# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest results.json**

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
raw_results = f"{mount_dict['raw']}/results.json"

print(processed_results, raw_results)

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Define Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType, FloatType

races_schema = StructType(fields=[
  StructField("resultId", IntegerType(), False),
  StructField("raceId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("constructorId", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("grid", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("positionText", StringType(), True),
  StructField("positionOrder", IntegerType(), True),
  StructField("points", FloatType(), True),
  StructField("laps", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("fastestLap", IntegerType(), True),
  StructField("rank", IntegerType(), True),
  StructField("fastestLapTime", StringType(), True),
  StructField("fastestLapSpeed", StringType(), True),
  StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC **5. READ the JSON file using the spark dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

results_df = spark.read.json(raw_results, schema=races_schema)

display(results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **6. Drop Unwnated Column**

# COMMAND ----------

df_drop = results_df.drop("statusId")


# COMMAND ----------

# MAGIC %md
# MAGIC **7. Rename Columns**

# COMMAND ----------

from pyspark.sql.functions import col
df_rename = df_drop.select(
  col("resultId").alias("result_id"),
  col("raceId").alias("race_id"),
  col("driverId").alias("driver_id"),
  col("constructorId").alias("constructor_id"),
  col("number"),
  col("grid"),
  col("position"),
  col("positionText").alias("position_text"),
  col("positionOrder").alias("position_order"),
  col("points"),
  col("laps"),
  col("time"),
  col("milliseconds"),
  col("fastestLap").alias("fastest_lap"),
  col("rank"),
  col("fastestLapTime").alias("fastest_lap_time"),
  col("fastestLapSpeed").alias("fastest_lap_speed")
)


# COMMAND ----------

# MAGIC %md
# MAGIC **8. Add Timestamp**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit 
final_df = df_rename.withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **9. Write Parquet to Processed**

# COMMAND ----------

final_df.write.mode("overwrite").parquet(processed_results)

# COMMAND ----------

try: 
    final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_results):
            dbutils.fs.rm(processed_results, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **10. Check Parquet Written**

# COMMAND ----------

display(spark.read.parquet(processed_results))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
