# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest constructors.csv**

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
# MAGIC %run "../includes/common_functions"

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
# MAGIC **Read the JSON file using the spark dataframe reader**

# COMMAND ----------

constructors_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"/mnt/{storage_account}/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop Unwanted Columns**

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename column and add ingestion_date**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write output to Parquet**

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"/mnt/{storage_account}/processed/constructors")
