# Define the provider:
provider "azurerm" {
    features {
        key_vault {
          purge_soft_delete_on_destroy = true
          recover_soft_deleted_key_vaults = false
        }
    }
}

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.48.2"
    }
  }
}

provider "databricks" {
  host = "https://${azurerm_databricks_workspace.workspace.workspace_url}"
  azure_workspace_resource_id = azurerm_databricks_workspace.workspace.id
}

# Define the resource group:
resource "azurerm_resource_group" "azure_databricks" {
  name     = "databrickscourse-rg"
  location = "UK South"
}

# Dynamically gain tenant_id key:
data "azurerm_client_config" "current" {}

 
  