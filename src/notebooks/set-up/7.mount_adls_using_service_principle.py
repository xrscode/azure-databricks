# Databricks notebook source
# MAGIC %md
# MAGIC **Mount Azure Data Lake using Service Principal.**
# MAGIC
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Get Spark Config with App/ Client Id, Directory/ Tenant ID & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilties related to mount (list all mounts, unmount)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Set Variables**

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
container_name = 'demo'
scope_name = 'f1-scope'

# COMMAND ----------

# MAGIC %md
# MAGIC **Configure spark.conf**

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC **Mount**

# COMMAND ----------

dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
    mount_point=f"/mnt/{storage_account}/{container_name}",
    extra_configs=configs)

# COMMAND ----------

# MAGIC %md
# MAGIC **List files from demo container:**

# COMMAND ----------

# "DemoAccountName - keyvault Secret = 'demo'"
list = dbutils.fs.ls(
    f"/mnt/{storage_account}/{container_name}")
display(list)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data from circuits.csv**

# COMMAND ----------

# Display the file:
file = spark.read.csv(
    f"/mnt/{storage_account}/{container_name}")
display(file)

# COMMAND ----------

# MAGIC %md
# MAGIC **Show mounts**

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC **Unmount**

# COMMAND ----------

# dbutils.fs.unmount('/mnt/f1dl9072024/demo')
# display(dbutils.fs.mounts())
