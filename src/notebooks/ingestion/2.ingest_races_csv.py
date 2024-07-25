# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest Races.csv**

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
# MAGIC **Establish File Paths**

# COMMAND ----------

import json
# List files in the expected directory
files = dbutils.fs.ls("/mnt")

# Set File Location
file_path = "/dbfs/mnt/mount_dict.json"
with open(file_path, "r") as f:
    mount_dict = json.load(f)  

processed_races = f"{mount_dict['processed']}/races"
raw_races = f"{mount_dict['raw']}/races.csv"

print(processed_races, raw_races)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dl9072024/raw

# COMMAND ----------

# MAGIC %md
# MAGIC **Read to dataframe**

# COMMAND ----------

races_df = spark.read.csv(raw_races, header='true')

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
# MAGIC **Re-define 'races'**

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(raw_races)

# COMMAND ----------

# MAGIC %md
# MAGIC **Update Ingestion Date timestamp**
# MAGIC Note that here we are combining two columns; date and time into one using the 'concat' function.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_with_timestamp_df = races_df \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source))
display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Select columns** \
# MAGIC Drop 'URL'
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df = races_with_timestamp_df.select(
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

races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet(processed_races)

# COMMAND ----------

try: 
    final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        if dbutils.fs.ls(processed_races):
            dbutils.fs.rm(processed_races, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")
    except Exception as e:
        print(f"Exception occured: {e}")


# COMMAND ----------

display(spark.read.parquet(processed_races))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
