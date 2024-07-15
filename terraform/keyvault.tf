# Sets up keyvault resource:
resource "azurerm_key_vault" "f1keyvault" {
  name                        = "f1-key-vault-9072024"
  location                    = azurerm_resource_group.azure_databricks.location
  resource_group_name         = azurerm_resource_group.azure_databricks.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = ["Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore", "Decrypt", "Encrypt", "UnwrapKey", "WrapKey", "Verify", "Sign", "Purge", "Release", "Rotate", "GetRotationPolicy", "SetRotationPolicy"]

  secret_permissions = ["Get", "List", "Set", "Delete", "Recover", "Backup", "Restore", "Purge"]

  certificate_permissions = ["Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore", "ManageContacts", "ManageIssuers", "GetIssuers", "ListIssuers", "SetIssuers", "DeleteIssuers", "Purge"]
  }

  # Access policy for Databricks:
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    # Links to databricks:
    object_id = data.azurerm_client_config.current.subscription_id

    key_permissions = ["Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore", "Decrypt", "Encrypt", "UnwrapKey", "WrapKey", "Verify", "Sign", "Purge", "Release", "Rotate", "GetRotationPolicy", "SetRotationPolicy"]

    secret_permissions = ["Get", "List", "Set", "Delete", "Recover", "Backup", "Restore", "Purge"]

    storage_permissions = [
      "Get",
      "List"
    ]
  }
  depends_on = [azurerm_databricks_workspace.workspace]
}

# Store the Databricks host URL as a secret in Key Vault
resource "azurerm_key_vault_secret" "databricks_host_url" {
  name         = "databricks-url"
  value        = azurerm_databricks_workspace.workspace.workspace_url
  key_vault_id = azurerm_key_vault.f1keyvault.id
}

# Store the Databricks PAT as a secret key in Key Vault:
resource "azurerm_key_vault_secret" "databricks_pat" {
  name         = "databricks-pat"
  value        = databricks_token.pat.token_value
  key_vault_id = azurerm_key_vault.f1keyvault.id
  depends_on   = [databricks_token.pat]
}

# Store name of demo storage account in key vault:
resource "azurerm_key_vault_secret" "storage_account_name_demo" {
  name         = "DemoAccountName"
  value        = azurerm_storage_container.demo.name
  key_vault_id = azurerm_key_vault.f1keyvault.id
  depends_on = [ azurerm_storage_container.demo ]
}

# Store name of storage account as secret:
resource "azurerm_key_vault_secret" "storage_account_name_secret" {
  name         = "storage-account"
  value        = azurerm_storage_account.storage_account_one.name
  key_vault_id = azurerm_key_vault.f1keyvault.id
}

# Store the primary access key in Key Vault
resource "azurerm_key_vault_secret" "storage_account_primary_key" {
  name         = "storage-account-primary-key"
  value        = azurerm_storage_account.storage_account_one.primary_access_key
  key_vault_id = azurerm_key_vault.f1keyvault.id
}

# Store SAS token in key vault:
resource "azurerm_key_vault_secret" "sas_token_demo" {
    name = "sas-demo"
    value = data.azurerm_storage_account_blob_container_sas.sas_demo.sas
    key_vault_id = azurerm_key_vault.f1keyvault.id
}