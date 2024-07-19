# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest Circuits.csv**

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
csv_location = "dbfs:/mnt/f1dl9072024/raw/races.csv"

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
# MAGIC Establish directory of mountpoint:

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dl9072024/raw

# COMMAND ----------

# MAGIC %md
# MAGIC **Read to dataframe**

# COMMAND ----------

races_df = spark.read.csv("dbfs:/mnt/f1dl9072024/raw/races.csv", header='true')

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
.csv(csv_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **Update Ingestion Date timestamp**
# MAGIC Note that here we are combining two columns; date and time into one using the 'concat' function.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_with_timestamp_df = races_df \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
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

file_path = f"/mnt/{storage_account}/processed/races"
races_selected_df.write.mode("overwrite").parquet(file_path)

# COMMAND ----------

display(spark.read.parquet(file_path))
