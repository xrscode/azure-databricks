resource "databricks_cluster" "cluster" {
    cluster_name = "f1-cluster"
    node_type_id = "Standard_DS3_v2"
    spark_version = "14.3.x-scala2.12"
    autotermination_minutes = 60
    # CONFIGURATION FOR SINGLE NODE CLUSTER!!!!
    num_workers = 0
    spark_conf = {
    # Single-node
     "spark.master": "local[*, 4]",
     "spark.databricks.cluster.profile": "singleNode"
  } 
    custom_tags = {
    "ResourceClass" = "SingleNode"
  }
  depends_on = [azurerm_storage_account.storage_account_one, databricks_secret_scope.dbs_secret, azurerm_key_vault_secret.storage_account_primary_key]
}



# CLUSTER SCOPED AUTHENTICATION (VIA ACCESS KEYS)
resource "databricks_cluster" "scoped_cluster" {
    cluster_name = "f1-cluster-scoped-authentication"
    node_type_id = "Standard_DS3_v2"
    spark_version = "14.3.x-scala2.12"
    autotermination_minutes = 25
    # CONFIGURATION FOR SINGLE NODE CLUSTER!!!!
    num_workers = 0
    spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*, 4]",
    # Access to key vault for primary key:
    "fs.azure.account.key.${azurerm_storage_account.storage_account_one.name}.dfs.core.windows.net" : "{{secrets/f1-scope/storage-account-primary-key}}"
  } 
    custom_tags = {
    "ResourceClass" = "SingleNode"
  }
  # Requires secret scope & primary key stored in key vault:
  depends_on = [azurerm_storage_account.storage_account_one, databricks_secret_scope.dbs_secret, azurerm_key_vault_secret.storage_account_primary_key]
}