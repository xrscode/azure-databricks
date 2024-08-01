# Databricks notebook source
# MAGIC %md
# MAGIC **Delta Lake Demo**
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (file)

# COMMAND ----------

# %run "../set-up/8.mount_adls_containers_for_project"

# COMMAND ----------

import json

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

results_location = f"{mount_dict['raw_increment']}/2021-03-28/results.json"
write_demo = f"{mount_dict['demo']}"
managed_table = "dbfs:/mnt/f1dl9072024/demo/results_managed"
drivers_table = f"{mount_dict['raw_increment']}/2021-03-28/drivers.json"
drivers_merge = f"{mount_dict['demo']}/drivers_merge"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS f1_demo CASCADE;
# MAGIC DROP DATABASE IF EXISTS f1_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE f1_demo
# MAGIC LOCATION "/mnt/f1dl9072024/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json(results_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data to delta lake - managed table**

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data to delta lake - external table**

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save(f"{write_demo}/results_external")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create external table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/f1dl9072024/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/f1dl9072024/demo/results_external")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC **Updates and Delete**
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Update Delta Table using SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Update Delta Table using Python:

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "dbfs:/mnt/f1dl9072024/demo/results_managed")

deltaTable.update("position <= 10", {"points": "21 - position"})


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC **DELETE Delta Lake**

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Delete:

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %md
# MAGIC PYTHON Delete:

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, managed_table)

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC **UPSERT**

# COMMAND ----------

# MAGIC %md
# MAGIC UPSERT using merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json(drivers_table) \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")    

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json(drivers_table) \
.filter("driverId BETWEEN 6 AND 16") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json(drivers_table) \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 and 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")
drivers_day2_df.createOrReplaceTempView("drivers_day2")
drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %md
# MAGIC Create table: f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate Date
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC MERGE

# COMMAND ----------

# MAGIC %md
# MAGIC **DAY 1:**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC **DAY 2**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC **DAY 3**

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, drivers_merge)

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
.whenMatchedUpdate(set = {"dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname", "updatedDate": current_timestamp()}) \
.whenNotMatchedInsert( values = 
                      {
                          "driverId": "upd.driverId",
                          "dob": "upd.dob",
                          "forename": "upd.forename",
                          "surname": "upd.surname",
                          "createdDate": "current_timestamp()"
                      }) \
.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC **History, Time Travel, Vacuum**
# MAGIC 1. History & Versioning.
# MAGIC 2. Time Travel
# MAGIC 3. Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-08-01T09:17:26.000+00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Version

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2024-08-01T09:17:26.000+00:00").load(drivers_merge)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Delete** \
# MAGIC Vacuum:

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-08-01T09:17:26.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS;

# COMMAND ----------

# %sql
# SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-08-01T09:17:26.000+00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC Demonstrate deletion and restore:

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# %sql
# SELECT * FROM f1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# %sql
# MERGE INTO f1_demo.drivers_merge tgt
# USING f1_demo.drivers_merge VERSION AS OF 3 src
#   ON (tgt.driverId = src.driverId)
# WHEN NOT MATCHED THEN
#  INSERT *

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Table 'drivers_txn**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note: ensure that drivers_merge exists.
# MAGIC
# MAGIC This will add one row to drivers_txn.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert Parquet to Delta**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC Creates a parquet file:

# COMMAND ----------

df.write.format("parquet").save("dbfs:/mnt/f1dl9072024/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %md
# MAGIC Converts parquet file into Delta:

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/f1dl9072024/demo/drivers_convert_to_delta_new`

# COMMAND ----------

# MAGIC %md
# MAGIC
