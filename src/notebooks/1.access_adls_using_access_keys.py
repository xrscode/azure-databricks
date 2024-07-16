# Databricks notebook source
# MAGIC %md
# MAGIC **Access Azure Data Lake using access keys**
# MAGIC
# MAGIC 1. Set the spark config fs.azure.account.key.
# MAGIC 2. List files from demo container.
# MAGIC 3. Read data from circuits.csv file.

# COMMAND ----------

# Access secret:
secret = dbutils.secrets.get(
    scope="f1-scope", key="storage-account-primary-key")

storage_account_name = "f1dl9072024"
container_name = 'demo'

# Set the spark configuration:
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    secret
)

# COMMAND ----------

# MAGIC %md
# MAGIC **List files from demo container:**

# COMMAND ----------

# "DemoAccountName - keyvault Secret = 'demo'"
list = dbutils.fs.ls(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net")
display(list)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data from circuits.csv**

# COMMAND ----------

# Display the file:
file = spark.read.csv(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv")
display(file)
