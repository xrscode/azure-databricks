# Databricks notebook source
# MAGIC %md
# MAGIC **Set Spark Config**

# COMMAND ----------

client_id = dbutils.secrets.get(
    scope="f1-scope", key="application-client-id-demo")
tenant_id = dbutils.secrets.get(
    scope="f1-scope", key="directory-tenant-id-demo")
client_secret = dbutils.secrets.get(
    scope="f1-scope", key="application-client-secret")
storage_account_name = "f1dl9072024"
container_name = "processed"

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


# COMMAND ----------

# MAGIC %md
# MAGIC **Create simple Dataframe & Write to Processed**

# COMMAND ----------

# Create a simple DataFrame
df = spark.createDataFrame([("Hello", "World"), ("Databricks", "Rocks")], ["Column1", "Column2"])

# Write the DataFrame to a CSV file in the mounted directory
df.write.mode("overwrite").csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/simple_file.csv")

# Verify that the file has been written
display(dbutils.fs.ls("/mnt/processed"))
