# Define databricks workspace
resource "azurerm_databricks_workspace" "workspace" {
    name = "databrickscourse-ws"
    resource_group_name = azurerm_resource_group.azure_databricks.name
    location = azurerm_resource_group.azure_databricks.location
    sku = "standard"
}

# Output the Databricks workspace URL
output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.workspace.workspace_url
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