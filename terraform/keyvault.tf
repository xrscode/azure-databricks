# Sets up keyvault resource:
# resource "azurerm_key_vault" "f1keyvault" {
#   name                        = "f1-key-vault-9072024"
#   location                    = azurerm_resource_group.azure_databricks.location
#   resource_group_name         = azurerm_resource_group.azure_databricks.name
#   enabled_for_disk_encryption = true
#   tenant_id                   = data.azurerm_client_config.current.tenant_id
#   soft_delete_retention_days  = 7
#   purge_protection_enabled    = false

#   sku_name = "standard"

#   access_policy {
#     tenant_id = data.azurerm_client_config.current.tenant_id
#     object_id = data.azurerm_client_config.current.object_id

#     key_permissions = [
#       "Get",
#       "Delete",
#       "Purge",
#       "Create"
#     ]

#     secret_permissions = [
#       "Get",
#       "Delete",
#       "Purge",
#       "Set"
#     ]

#     storage_permissions = [
#       "Get",
#       "Delete",
#       "Purge",
#       "List",
#       "Set",
#       "Update",
#       "Purge"
#     ]
#   }

#   # Access policy for Databricks:
#   access_policy {
#     tenant_id = data.azurerm_client_config.current.tenant_id
#     object_id = data.azurerm_client_config.current.subscription_id

#     key_permissions = [
#       "Get",
#       "List"
#     ]

#     secret_permissions = [
#       "Get",
#       "List"
#     ]

#     storage_permissions = [
#       "Get",
#       "List"
#     ]
#   }
#   depends_on = [azurerm_databricks_workspace.workspace]
# }

# # Store the Databricks host URL as a secret in Key Vault
# resource "azurerm_key_vault_secret" "databricks_host_url" {
#   name         = "databricks-url"
#   value        = azurerm_databricks_workspace.workspace.workspace_url
#   key_vault_id = azurerm_key_vault.f1keyvault.id
# }

# Store the Databricks PAT as a secret key in Key Vault:
# resource "azurerm_key_vault_secret" "databricks_pat" {
#   name         = "databricks-pat"
#   value        = databricks_token.pat.token_value
#   key_vault_id = azurerm_key_vault.f1keyvault.id
#   depends_on   = [databricks_token.pat]
# }

