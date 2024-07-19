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
csv_location = "dbfs:/mnt/f1dl9072024/raw/circuits.csv"

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
# MAGIC **4. Display Mount Points**
# MAGIC Locate: '/mnt/f1dl9072024/raw

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC **5. List files in: /mnt/f1dl9072024/raw**

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dl9072024/raw

# COMMAND ----------

# MAGIC %md
# MAGIC **6. Save circuits.csv to variable**

# COMMAND ----------

circuits_df_one = spark.read.csv("dbfs:/mnt/f1dl9072024/raw/circuits.csv", header='true')

# Displays the type of 'circuits_df'.  pyspark.sql.dataframe.DataFrame
display(type(circuits_df_one))

# Shows the tables:
display(circuits_df_one.show())


# COMMAND ----------

# MAGIC %md
# MAGIC **7. Print Schema**
# MAGIC Provides information about the schema.

# COMMAND ----------

circuits_df_one.printSchema()



# COMMAND ----------

# Perform a summary of basic descriptive statistics:
circuits_df_one.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **8. inferSchema**
# MAGIC inferSchema can be used to automatically detect the data types of each column in the dataset.

# COMMAND ----------

# Save variable with inferSchema set to true:
circuits_df = spark.read.csv("dbfs:/mnt/f1dl9072024/raw/circuits.csv", header='true', inferSchema='true')
circuits_df.printSchema()
circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **9. import from Pyspark**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC **10. Specify Schema**

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC **11. Re-define 'circuits_df'**

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(csv_location)
display(circuits_df)  

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **11. Select columns** \
# MAGIC There are some columns we do not need.  We need to drop them. \
# MAGIC One method is to use; DataFrame.select

# COMMAND ----------

# # Method 1:
# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
# display(circuits_selected_df)


# COMMAND ----------

# # Method 2:
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
# display(circuits_selected_df)

# COMMAND ----------

# # Method 3:
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])
# display(circuits_selected_df)

# COMMAND ----------

# Method 4: requires 'col' to be imported
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **12. Rename Columns**

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")
circuits_renamed_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **13. Add Data**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **14. Write to DataLake as Parquet**

# COMMAND ----------

file_path = "/mnt/formula1dl/processed/circuits"
circuits_final_df.write.mode("overwrite").parquet(file_path)

# COMMAND ----------

# %fs
# ls /mnt/formula1dl/processed/circuits

# COMMAND ----------

# # Read parquet to verify:
# display(spark.read.parquet(file_path))
