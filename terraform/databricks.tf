# Define databricks workspace
resource "azurerm_databricks_workspace" "workspace" {
    name = "databrickscourse-ws"
    resource_group_name = azurerm_resource_group.azure_databricks.name
    location = azurerm_resource_group.azure_databricks.location
    sku = "standard"
    # !!!!!!!!!!!
    depends_on = [azurerm_key_vault.f1keyvault]
}

# Output the Databricks workspace URL
output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.workspace.workspace_url
}

# Output the Databricks ID:
output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.workspace.id
}

# Output the config object_id
output "azure_current_object_id"{
  value = data.azurerm_client_config.current.subscription_id
}

# Create a Databricks personal access token, ensuring it depends on the workspace creation:
resource "databricks_token" "pat" {
  comment         = "Terraform Provisioning"
  lifetime_seconds = 8640000  # 100-day token
  depends_on       = [azurerm_databricks_workspace.workspace]
}

# Output the Databricks personal access token:
output "databricks_access_token" {
  value      = databricks_token.pat.token_value
  sensitive  = true
}

# Create Secret Scope
# Will allow data bricks to connect to secret vault:
resource "databricks_secret_scope" "dbs_secret" {
  name = "f1-scope"
  initial_manage_principal = "users"
  keyvault_metadata {
    resource_id = azurerm_key_vault.f1keyvault.id
    dns_name    = azurerm_key_vault.f1keyvault.vault_uri
  }
  depends_on = [ azurerm_key_vault_access_policy.one, azurerm_key_vault_access_policy.two, azurerm_key_vault.f1keyvault, azurerm_databricks_workspace.workspace, azurerm_key_vault_secret.storage_account_primary_key]
}

# Outputs the resource ID:
output "resource_id" {
  value = azurerm_key_vault.f1keyvault.id
}

# Outputs the DNS name:
output "dns_name" {
  value = azurerm_key_vault.f1keyvault.vault_uri
}

