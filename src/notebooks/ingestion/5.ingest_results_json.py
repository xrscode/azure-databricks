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
# MAGIC **1. Establish credentials to allow mount to Blob Storage:**
# MAGIC

# COMMAND ----------
# Access variables stored in key vault:
# Access application-client-id token secret:
client_id = dbutils.secrets.get(
    scope="f1-scope", key="application-client-id-demo")
tenant_id = dbutils.secrets.get(
    scope="f1-scope", key="directory-tenant-id-demo")
client_secret = dbutils.secrets.get(
    scope="f1-scope", key="application-client-secret")
storage_account = "f1dl9072024"
container_name = 'raw'
scope_name = 'f1-scope'
json_location = "dbfs:/mnt/f1dl9072024/raw/results.json"

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **Configure Spark**

# COMMAND ----------
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Mount**
# MAGIC Note you can call a function from another notebook:

# COMMAND ----------
def mount_adls(storage_account_name, container_name):
    # Access secrets from Key Vault:
    client_id = dbutils.secrets.get(
        scope="f1-scope", key="application-client-id-demo")
    tenant_id = dbutils.secrets.get(
        scope="f1-scope", key="directory-tenant-id-demo")
    client_secret = dbutils.secrets.get(
        scope="f1-scope", key="application-client-secret")

    # Set spark configurations:
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Check to see if mount exists.  Unmount if exists:
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs)


mount_adls(storage_account, container_name)

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

results_df = spark.read.json(f"/mnt/{storage_account}/raw/results.json", schema=races_schema)

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
df_with_column = df_rename.withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **9. Write Parquet to Processed**

# COMMAND ----------
df_with_column.write.mode("overwrite").parquet(f"/mnt/{storage_account}/processed/results")

# COMMAND ----------

# MAGIC %md
# MAGIC **10. Check Parquet Written**

# COMMAND ----------
display(spark.read.parquet(f"/mnt/{storage_account}/processed/results"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------
dbutils.notebook.exit("Success")
