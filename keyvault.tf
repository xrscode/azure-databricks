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
  depends_on = [azurerm_storage_account.storage_account_one]

}

resource "azurerm_key_vault_access_policy" "one" {
  key_vault_id = azurerm_key_vault.f1keyvault.id
  tenant_id = data.azurerm_client_config.current.tenant_id
  object_id = data.azurerm_client_config.current.object_id
  key_permissions = ["Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore", "Decrypt", "Encrypt", "UnwrapKey", "WrapKey", "Verify", "Sign", "Purge", "Release", "Rotate", "GetRotationPolicy", "SetRotationPolicy"]
  secret_permissions = ["Get", "List", "Set", "Delete", "Recover", "Backup", "Restore", "Purge"]
  certificate_permissions = ["Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore", "ManageContacts", "ManageIssuers", "GetIssuers", "ListIssuers", "SetIssuers", "DeleteIssuers", "Purge"]
  storage_permissions = [ "Get","List"]
  depends_on = [azurerm_key_vault.f1keyvault]
}

resource "azurerm_key_vault_access_policy" "two" {
  key_vault_id = azurerm_key_vault.f1keyvault.id
  tenant_id = data.azurerm_client_config.current.tenant_id
  # Links to databricks:
  object_id = data.azurerm_client_config.current.subscription_id
  key_permissions = ["Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore", "Decrypt", "Encrypt", "UnwrapKey", "WrapKey", "Verify", "Sign", "Purge", "Release", "Rotate", "GetRotationPolicy", "SetRotationPolicy"]
  secret_permissions = ["Get", "List", "Set", "Delete", "Recover", "Backup", "Restore", "Purge"]
  storage_permissions = [ "Get","List"]
  depends_on = [azurerm_key_vault.f1keyvault]
}


# Store the Databricks host URL as a secret in Key Vault
resource "azurerm_key_vault_secret" "databricks_host_url" {
  name         = "databricks-url"
  value        = azurerm_databricks_workspace.workspace.workspace_url
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and databricks workspace creation:
  depends_on = [azurerm_key_vault.f1keyvault, azurerm_databricks_workspace.workspace, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two]
}

# Store the Databricks PAT as a secret key in Key Vault:
resource "azurerm_key_vault_secret" "databricks_pat" {
  name         = "databricks-pat"
  value        = databricks_token.pat.token_value
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and databricks token creation:
  depends_on   = [azurerm_key_vault.f1keyvault, databricks_token.pat, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two]
}

# Store name of demo storage account in key vault:
resource "azurerm_key_vault_secret" "storage_account_name_demo" {
  name         = "DemoAccountName"
  value        = azurerm_storage_container.demo.name
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and storage container 'demo' creation:
  depends_on = [ azurerm_key_vault.f1keyvault, azurerm_storage_container.demo, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two ]
}

# Store name of storage account as secret:
resource "azurerm_key_vault_secret" "storage_account_name_secret" {
  name         = "storage-account"
  value        = azurerm_storage_account.storage_account_one.name
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and storage account creation:
  depends_on = [azurerm_key_vault.f1keyvault, azurerm_storage_account.storage_account_one, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two]
}

# Store the primary access key in Key Vault
resource "azurerm_key_vault_secret" "storage_account_primary_key" {
  name         = "storage-account-primary-key"
  value        = azurerm_storage_account.storage_account_one.primary_access_key
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and storage account creation:
  depends_on = [azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two, azurerm_key_vault.f1keyvault, azurerm_storage_account.storage_account_one]
}

# Store SAS token in key vault:
resource "azurerm_key_vault_secret" "sas_token_demo" {
    name = "sas-demo"
    value = data.azurerm_storage_account_blob_container_sas.sas_demo.sas
    key_vault_id = azurerm_key_vault.f1keyvault.id
    # Requires key vault and sas token creation:
    depends_on = [azurerm_key_vault.f1keyvault, data.azurerm_storage_account_blob_container_sas.sas_demo, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two]
}

# Store App User Information:
resource "azurerm_key_vault_secret" "app_client_id" {
  name         = "application-client-id-demo"
  value        = azuread_application.setup.client_id
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and azureread:
  depends_on = [ azurerm_key_vault.f1keyvault, azuread_application.setup, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two ]
}
resource "azurerm_key_vault_secret" "tenant_id" {
  name         = "directory-tenant-id-demo"
  value        = data.azurerm_client_config.current.tenant_id
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and azureread:
  depends_on = [ azurerm_key_vault.f1keyvault, azuread_application.setup, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two ]
}
resource "azurerm_key_vault_secret" "client_secret" {
  name         = "application-client-secret"
  value        = azuread_application_password.setup.value
  key_vault_id = azurerm_key_vault.f1keyvault.id
  # Requires key vault and azureread:
  depends_on = [ azurerm_key_vault.f1keyvault, azuread_application.setup, azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two ]
}