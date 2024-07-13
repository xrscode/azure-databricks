# Reference the existing Key Vault
data "azurerm_key_vault" "existing" {
  name                = var.key_vault  # The name of your existing Key Vault
  resource_group_name = "databrickscourse-rg"  # The resource group where the Key Vault exists
}

# Store the Databricks token as a secret in the existing Key Vault
resource "azurerm_key_vault_secret" "databricks_token" {
  name         = "databricks-token"
  value        = var.databricks_token  # Reference the token from your .tfvars file
  key_vault_id = data.azurerm_key_vault.existing.id
}