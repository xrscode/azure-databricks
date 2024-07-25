# Databricks notebook source
# MAGIC %md
# MAGIC **Mount Azure Data Lake Containers for the Project.**
# MAGIC
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Get Spark Config with App/ Client Id, Directory/ Tenant ID & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilties related to mount (list all mounts, unmount)

# COMMAND ----------

storage_account = "f1dl9072024"
scope_name = 'f1-scope'

def mount_adls(storage_account_name, container_name):
    """
    Args: storage account name, container name.
    Returns: mountpoint as string.
    """
    # Access secrets from Key Vault:
    client_id = dbutils.secrets.get(
    scope="f1-scope", key="application-client-id-demo")
    tenant_id = dbutils.secrets.get(
    scope="f1-scope", key="directory-tenant-id-demo")
    client_secret = dbutils.secrets.get(
    scope="f1-scope", key="application-client-secret")

    mount_point = f"/mnt/{storage_account_name}/{container_name}"

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
    return mount_point


# COMMAND ----------

# MAGIC %md
# MAGIC **Mount Raw / Processed / Presentation Container**

# COMMAND ----------


mount_dict = {"raw": mount_adls(storage_account, "raw"), "presentation": mount_adls(storage_account, "presentation"), "processed": mount_adls(storage_account, "processed")}

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify Container Dictionary**

# COMMAND ----------

print(mount_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC **Save Dictionary to Secret Scope** \
# MAGIC Saving dictionary to secret scope allows other notebooks to access it.

# COMMAND ----------

import json
db_scope = "f1-scope"

# Convert dictionary to JSON string:
mount_dict_json = json.dumps(mount_dict)

# Save JSON string within DBFS file system:
dbutils.fs.put("/mnt/mount_dict.json", json.dumps(mount_dict), overwrite=True)




# COMMAND ----------

# MAGIC %md
# MAGIC
