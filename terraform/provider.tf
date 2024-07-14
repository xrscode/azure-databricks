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
      version = "1.0.0"
    }
  }
}

# Define the resource group:
resource "azurerm_resource_group" "azure_databricks" {
  name     = "databrickscourse-rg"
  location = "UK South"
}

# Dynamically gain tenant_id key:
data "azurerm_client_config" "current" {}

# Create a key vault:
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

    key_permissions = [
      "Get",
      "Delete",
      "Purge",
      "Create"
    ]

    secret_permissions = [
      "Get",
      "Delete",
      "Purge",
      "Set"
    ]

    storage_permissions = [
      "Get",
      "Delete",
      "Purge",
      "List",
      "Set",
      "Update",
      "Purge"
    ]
  }

  # Access policy for Databricks:
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_databricks_workspace.workspace.principal_id 

    key_permissions = [
      "Get",
      "List"
    ]

    secret_permissions = [
      "Get",
      "List"
    ]

    storage_permissions = [
      "Get",
      "List"
    ]
  }
}


 
  