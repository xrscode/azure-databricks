# Databricks notebook source
# MAGIC %md
# MAGIC **Access Azure Data Lake using SAS token.**
# MAGIC
# MAGIC 1. Register Azure AD Application / Service Principal.
# MAGIC   Go to Microsoft Entra ID.
# MAGIC   Manage > App Registrations > {fill out info} > Register
# MAGIC   Copy client_id, tenant_id and store as variables.
# MAGIC 2. Generate a secret/password for the Application
# MAGIC   From Microsoft Entra ID.
# MAGIC   Manage > Certificates & Secrets > {fill out info}
# MAGIC   Save '**SECRET VALUE**' to variable.
# MAGIC 3. Set Spark Config with App / Client ID, Directory / Tenant ID and Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

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

# COMMAND ----------

# MAGIC %md
# MAGIC **Configure spark.conf**

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", f"{client_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

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
