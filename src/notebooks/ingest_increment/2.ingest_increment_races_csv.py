# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest Increment Races**

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

raw_increment_races = f"{mount_dict['raw_increment']}/{v_file_date}/races.csv"
processed_races = f"{mount_dict['processed']}/races"

dbs = "f1_processed"
tbl = "races"
clm = "race_id"   

# COMMAND ----------

# MAGIC %md
# MAGIC **Specify schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

races_schema = StructType(fields = [
  StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Dataframe**

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(raw_increment_races)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add ingestion_date, race_timestamp, data_source**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_with_timestamp_df = races_df \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC **Select columns** \
# MAGIC Drop 'URL'
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
final_df = races_with_timestamp_df.select(
  col('raceId').alias("race_id"),
  col('year').alias("race_year"),
  col('round'),
  col('circuitId').alias("circuit_id"),
  col('name'),
  col('ingestion_date'),
  col('race_timestamp')
)



# COMMAND ----------

# MAGIC %md
# MAGIC **Write to DataLake as Parquet**
# MAGIC

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy('race_year').parquet(processed_races)

# COMMAND ----------

try: 
    final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_races):
            dbutils.fs.rm(processed_races, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")
        print("Races table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")