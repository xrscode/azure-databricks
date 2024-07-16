# Databricks notebook source
# MAGIC %md
# MAGIC **Access Azure Data Lake using SAS token.**
# MAGIC
# MAGIC 1. Access SAS token from Key Vault.
# MAGIC 2. Set variables.
# MAGIC 3. Configure spark.conf.set.....

# COMMAND ----------

# Access sas token secret:
sas_token = dbutils.secrets.get(
    scope="f1-scope", key="sas-demo")
# sas_token currently referencing secret key.  
# Can replace sas_token if necessary. 

# Set variables:
storage_account_name = "f1dl9072024"
container_name = 'demo'

# COMMAND ----------

# MAGIC %md
# MAGIC Configure spark.conf

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", f"{sas_token}")

# COMMAND ----------

# MAGIC %md
# MAGIC **List files from demo container:**

# COMMAND ----------

# "DemoAccountName - keyvault Secret = 'demo'"
list = dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net")
display(list)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data from circuits.csv**

# COMMAND ----------

# Display the file:
file = spark.read.csv(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv")
display(file)
