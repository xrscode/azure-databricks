# Databricks notebook source
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
# MAGIC **Ingest Increment Results.json**

# COMMAND ----------

# Create Widget
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

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
raw_results = f"{mount_dict['raw_increment']}"

dbs = "f1_processed"
tbl = "results"
clm = "race_id"

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Schema**

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
  StructField("fastestLapSpeed", FloatType(), True),
  StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC **READ the JSON file using the spark dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

results_df = spark.read.json(f"{raw_results}/{v_file_date}/results.json", schema=races_schema)

display(results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **Drop Unwnated Column**

# COMMAND ----------

df_drop = results_df.drop("statusId")


# COMMAND ----------

# MAGIC %md
# MAGIC **Rename Columns**

# COMMAND ----------

from pyspark.sql.functions import col, lit
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
).withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC **Add Timestamp**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit 
final_df = df_rename.withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental Load Methods**

# COMMAND ----------

# MAGIC %md
# MAGIC **METHOD 1:**

# COMMAND ----------

# # Collect converts into list
# # Note: only use collect on small amount of data!
# for race_id_list in final_df.select("race_id").distinct().collect():
#     # Check if table exists.  Protects against non existant table:
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")
# final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC **METHOD 2:**

# COMMAND ----------

partition_list = overwrite_partition(clm, final_df)

select_final_df = final_df.select(partition_list)

# COMMAND ----------

partition_overwrite_mode_set = spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
partition_overwrite_mode_get = spark.conf.get("spark.sql.sources.partitionOverwriteMode")

# COMMAND ----------

print(partition_overwrite_mode_get)

# COMMAND ----------

display(select_final_df)

# COMMAND ----------

incremental_load(dbs, tbl, col)

# COMMAND ----------

# MAGIC %md
# MAGIC **Check Parquet Written**

# COMMAND ----------

display(spark.read.parquet(processed_results))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS f1_processed.results;

# COMMAND ----------

exists = spark._jsparkSession.catalog().tableExists(f"{tbl}.{clm}")
print(exists)
