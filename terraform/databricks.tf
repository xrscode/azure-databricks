# Define databricks workspace
resource "azurerm_databricks_workspace" "workspace" {
    name = "databrickscourse-ws"
    resource_group_name = azurerm_resource_group.azure_databricks.name
    location = azurerm_resource_group.azure_databricks.location
    sku = "standard"
}


# Store the Databricks host URL as a secret in Key Vault
resource "azurerm_key_vault_secret" "databricks_host_url" {
  name         = "databricks-url"
  value        = azurerm_databricks_workspace.workspace.workspace_url
  key_vault_id = azurerm_key_vault.f1keyvault.id
}

# Output the Databricks workspace URL
output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.workspace.workspace_url
}
