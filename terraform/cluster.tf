resource "databricks_cluster" "cluster" {
    cluster_name = "f1-cluster"
    # 0.75 DBU/H
    # node_type_id = "Standard_DS3_v2"
    # 0.5 DBU/H
    node_type_id = "Standard_F4"
    spark_version = "14.3.x-scala2.12"
    autotermination_minutes = 35
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
  # depends_on = [azurerm_storage_account.storage_account_one, databricks_secret_scope.dbs_secret, azurerm_key_vault_secret.storage_account_primary_key]
  depends_on = [azurerm_key_vault.f1keyvault, azurerm_storage_account.storage_account_one, databricks_secret_scope.dbs_secret, azurerm_key_vault_secret.storage_account_primary_key, azurerm_key_vault_secret.databricks_pat, azurerm_key_vault_secret.sas_token_demo, azurerm_key_vault_secret.tenant_id, azurerm_key_vault_secret.client_secret]
}



# # CLUSTER SCOPED AUTHENTICATION (VIA ACCESS KEYS)
# resource "databricks_cluster" "scoped_cluster" {
#     cluster_name = "f1-cluster-scoped-authentication"
#     node_type_id = "Standard_F4"
#     spark_version = "14.3.x-scala2.12"
#     autotermination_minutes = 25
#     # CONFIGURATION FOR SINGLE NODE CLUSTER!!!!
#     num_workers = 0
#     spark_conf = {
#     # Single-node
#     "spark.databricks.cluster.profile": "singleNode",
#     "spark.master": "local[*, 4]",
#     # Access to key vault for primary key:
#     "fs.azure.account.key.${azurerm_storage_account.storage_account_one.name}.dfs.core.windows.net" : "{{secrets/f1-scope/storage-account-primary-key}}"
#   } 
#     custom_tags = {
#     "ResourceClass" = "SingleNode"
#   }
#   # PARTIALLY WORKS:
#   # depends_on = [azurerm_storage_account.storage_account_one, databricks_secret_scope.dbs_secret, azurerm_key_vault_secret.storage_account_primary_key]

#   #THIS WORKS:
#   # depends_on = [azurerm_storage_account.storage_account_one, databricks_secret_scope.dbs_secret, azurerm_key_vault_secret.storage_account_primary_key, databricks_cluster.cluster, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two]

#   # TRIAL:
#   # Requires Secret scope setup.  Primary key stored.  Access policies to key vault. 
#   depends_on = [databricks_secret_scope.dbs_secret, azurerm_key_vault_secret.storage_account_primary_key, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two]
# }