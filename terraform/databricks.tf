# Define databricks workspace
resource "azurerm_databricks_workspace" "workspace" {
    name = "databrickscourse-ws"
    resource_group_name = azurerm_resource_group.azure_databricks.name
    location = azurerm_resource_group.azure_databricks.location
    sku = "standard"
}

# Databricks URL
data "azurerm_databricks_workspace" "workspaceurl"{
    name = azurerm_databricks_workspace.workspace.name
    resource_group_name = azurerm_resource_group.azure_databricks.name
}


