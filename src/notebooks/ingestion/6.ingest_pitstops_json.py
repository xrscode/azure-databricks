# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest pitstops.json**

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
json_location = "dbfs:/mnt/f1dl9072024/raw/pit_stops.json"

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
 
pit_schema = StructType(fields=[ 
StructField("raceId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("stop", IntegerType(), True),
StructField("time", StringType(), True),
StructField("duration", StringType(), True),
StructField("milliseconds", IntegerType(), True)
]) 

# COMMAND ----------

# MAGIC %md
# MAGIC **5. Read multi-line JSON**

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_schema) \
.option("multiLine", True) \
.json(json_location)


# COMMAND ----------

# MAGIC %md
# MAGIC **6. Rename & Add Ingestion Date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = pit_stops_df \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **7. Write Parquet**

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"/mnt/{storage_account}/processed/pit_stops")

# COMMAND ----------

end_path = 'pit_stops'
 
try:
    final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.{end_path}")
    print(f"{end_path.capitalize()} table successfully created.")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        path = f"{processed_folder_path}/{end_path}"
        if dbutils.fs.ls(path):
            dbutils.fs.rm(path, True)
        final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.{end_path}")
        print(f"{end_path.capitalize()} table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **8. Check Parquet**

# COMMAND ----------

display(spark.read.parquet(f"/mnt/{storage_account}/processed/pit_stops"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
