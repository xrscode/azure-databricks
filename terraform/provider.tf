# Define the provider:
provider "azurerm" {
    features {
        key_vault {
          purge_soft_delete_on_destroy = true
          recover_soft_deleted_key_vaults = false
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
      "Purge"
    ]

    secret_permissions = [
      "Get",
      "Delete",
      "Purge"
    ]

    storage_permissions = [
      "Get",
      "Delete",
      "Purge"
    ]
  }
}
