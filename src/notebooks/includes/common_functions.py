# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def hello():
    print('Hello world!')

def add_ingestion_date(input_df):
    """
    Args: dataframe.
    Outputs: dataframe with additional collumn; 'ingestion_date' set to current time.
    """
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

def mount_adls(storage_account_name, container_name):
    """
    Args: Name of storage account and name of container
    Returns: Nothing.  

    This function will attempt to mount the 'container_name'.
    If mount already exists, it will unmount. 
    If mount does not exist / unmount completed; will mount folder.
    """
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
