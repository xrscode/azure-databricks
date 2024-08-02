resource "azurerm_data_factory" "adf" {
  name                = "databricks-course-adf-02082024"
  location            = azurerm_resource_group.azure_databricks.location
  resource_group_name = azurerm_resource_group.azure_databricks.name

  identity {
    type = "SystemAssigned"
  }
}

# Create IAM role in databrickscourse-ws
# Define the role assignment
resource "azurerm_role_assignment" "adf-databricks" {
  principal_id   = azurerm_data_factory.adf.identity[0].principal_id
  role_definition_name = "Contributor"  # or any other role
  scope          = azurerm_resource_group.azure_databricks.id
}

# Configure Linked Service
# Define the Databricks Linked Service in ADF
resource "azurerm_data_factory_linked_service_azure_databricks" "databricks" {
  name                = "databricks-linked-service"
  data_factory_id = azurerm_data_factory.adf.id
  existing_cluster_id = databricks_cluster.cluster.id
  description         = "Linked service to Azure Databricks"
  access_token = databricks_token.pat.token_value
  adb_domain   = "https://${azurerm_databricks_workspace.workspace.workspace_url}"
}

# Define the pipeline using the JSON definition
resource "azurerm_data_factory_pipeline" "ingestion_pipeline" {
  name                = "pl_ingest_formula1_data"
  data_factory_id   = azurerm_data_factory.adf.id 
}



