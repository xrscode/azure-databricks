# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest drivers.json**

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
json_location = "dbfs:/mnt/f1dl9072024/raw/drivers.json"

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
# MAGIC **READ the JSON file using the spark dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])
drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{json_location}")
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename Columns & Add New Columns**
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop the Unwanted Columns**
# MAGIC 1. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Parquet to processed container**

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"/mnt/{storage_account}/processed/drivers")


# COMMAND ----------

display(spark.read.parquet(f"/mnt/{storage_account}/processed/drivers"))

# COMMAND ----------

end_path = 'drivers'

try: 
    drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.{end_path}")
    print(f"{end_path.capitalize()} table successfully created.")
except Exception as e:
    print(f"Exception occurred: {e}")
    try:
        path = f"{processed_folder_path}/{end_path}"
        if dbutils.fs.ls(path):
            dbutils.fs.rm(path, True)
        drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_processed.{end_path}")
        print(f"{end_path.capitalize()} table successfully created.")
    except Exception as e:
        print(f"Exception occured: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Exit Command**\
# MAGIC If notebook succeeds output is; "Success"

# COMMAND ----------

dbutils.notebook.exit("Success")
