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

# This will register a new app registration in Microsoft Entra ID:
# Dynamically gain tenant_id key:
data "azurerm_client_config" "current" {}

# Create provider:
provider "azuread" {
  tenant_id = data.azurerm_client_config.current.tenant_id
}

# Creates Azure Active Directory Application (User):
resource "azuread_application" "setup" {
  display_name = "formula1-app"
}
# Create Service Principal for the Application
resource "azuread_service_principal" "setup" {
  client_id = azuread_application.setup.client_id
}
# Create a Client Secret for the Application
resource "azuread_application_password" "setup" {
  application_id = azuread_application.setup.id
  display_name          = "f1-secret"
  end_date_relative     = "8760h"  # 1 year
  depends_on = [ azuread_application.setup ]
}
# Assign Storage Blob Data Contributor Role to Service Principal
resource "azurerm_role_assignment" "blob_data_contributor" {
  principal_id   = azuread_service_principal.setup.object_id
  role_definition_name = "Storage Blob Data Contributor"
  scope          = azurerm_storage_account.storage_account_one.id
}


 
  