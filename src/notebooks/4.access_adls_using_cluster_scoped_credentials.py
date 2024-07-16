# Databricks notebook source
# MAGIC %md
# MAGIC **Access Azure Data Lake using cluster scoped credentials.**
# MAGIC
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from cicrucits.csv

# COMMAND ----------

# MAGIC %md
# MAGIC **Set Variables**

# COMMAND ----------

storage_account = "f1dl9072024"
container_name = 'demo'

# COMMAND ----------

# MAGIC %md
# MAGIC **List files from demo container:**

# COMMAND ----------

# "DemoAccountName - keyvault Secret = 'demo'"
list = dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net")
display(list)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data from circuits.csv**

# COMMAND ----------

# Display the file:
file = spark.read.csv(
    f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/circuits.csv")
display(file)
