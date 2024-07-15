# Create a storage account:
resource "azurerm_storage_account" "storage_account_one" {
    name = "f1dl9072024"
    resource_group_name = azurerm_resource_group.azure_databricks.name
    location = azurerm_resource_group.azure_databricks.location
    account_tier = "Standard"
    # Locally redundant storage:
    account_replication_type = "LRS"
    # Enable hierarchical namespace:
    is_hns_enabled = true
}

# Create containers:
# Create Raw container:
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.storage_account_one.name
  container_access_type = "private"
}

# Create Processed container:
resource "azurerm_storage_container" "processed" {
  name                  = "processed"
  storage_account_name  = azurerm_storage_account.storage_account_one.name
  container_access_type = "private"
}

# Create Presentation container:
resource "azurerm_storage_container" "presentation" {
  name                  = "presentation"
  storage_account_name  = azurerm_storage_account.storage_account_one.name
  container_access_type = "private"
}


# Create Demo container:
resource "azurerm_storage_container" "demo" {
  name                  = "demo"
  storage_account_name  = azurerm_storage_account.storage_account_one.name
  container_access_type = "private"
}

# Upload circuits .csv to Demo container:
resource "azurerm_storage_blob" "upload-circuits"{
    name = "circuits.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.demo.name
    type = "Block"
    source = "../files/circuits.csv"
}

# Generate SAS tokens:
data "azurerm_storage_account_blob_container_sas" "sas_demo" {
  connection_string = azurerm_storage_account.storage_account_one.primary_connection_string
  container_name    = azurerm_storage_container.demo.name
  https_only        = true


  start  = "2018-03-21"
  expiry = "2026-03-21"

  permissions {
    read   = true
    add    = false
    create = false
    write  = false
    delete = false
    list   = true
  }

  cache_control       = "max-age=5"
  content_disposition = "inline"
  content_encoding    = "deflate"
  content_language    = "en-US"
  content_type        = "application/json"
}

output "sas_url_query_string" {
  value = data.azurerm_storage_account_blob_container_sas.sas_demo.sas
  sensitive = true
}
