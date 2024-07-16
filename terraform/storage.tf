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

# Upload RAW Data to Data Lake
# Files to upload: 'circuits, races, constructors, drivers, results, pitstops, laptimes, qualifying.'

# Upload circuits .csv to raw container:
resource "azurerm_storage_blob" "upload-circuits-raw"{
    name = "circuits.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/circuits.csv"
}

# Upload constructors.json to raw container:
resource "azurerm_storage_blob" "upload-constructors-raw"{
    name = "constructors.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/constructors.json"
}

# Upload drivers.json to raw container:
resource "azurerm_storage_blob" "upload-drivers-raw"{
    name = "drivers.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/drivers.json"
}

# Upload pit_stops.json to raw container:
resource "azurerm_storage_blob" "upload-pit-stops-raw"{
    name = "pit_stops.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/pit_stops.json"
}

# Upload races.csv to raw container:
resource "azurerm_storage_blob" "upload-races-raw"{
    name = "races.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/races.csv"
}

# Upload results.json to raw container:
resource "azurerm_storage_blob" "upload-results-raw"{
    name = "results.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/results.json"
}

# Upload lap_times 1 to raw container:
resource "azurerm_storage_blob" "upload-lap-times-1-raw"{
    name = "/lap_times/lap_times_split_1.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/lap_times/lap_times_split_1.csv"
}

# Upload lap_times 2 to raw container:
resource "azurerm_storage_blob" "upload-lap-times-2-raw"{
    name = "/lap_times/lap_times_split_2.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/lap_times/lap_times_split_2.csv"
}

# Upload lap_times 3 to raw container:
resource "azurerm_storage_blob" "upload-lap-times-3-raw"{
    name = "/lap_times/lap_times_split_3.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/lap_times/lap_times_split_3.csv"
}

# Upload lap_times 4 to raw container:
resource "azurerm_storage_blob" "upload-lap-times-4-raw"{
    name = "/lap_times/lap_times_split_4.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/lap_times/lap_times_split_4.csv"
}

# Upload lap_times 5 to raw container:
resource "azurerm_storage_blob" "upload-lap-times-5-raw"{
    name = "/lap_times/lap_times_split_5.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/lap_times/lap_times_split_5.csv"
}

# Upload qualifying 1 to raw container:
resource "azurerm_storage_blob" "upload-qualifying-1-raw"{
    name = "/qualifying/qualifying_split_1.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/qualifying/qualifying_split_1.json"
}

# Upload qualifying 2 to raw container:
resource "azurerm_storage_blob" "upload-qualifying-2-raw"{
    name = "/qualifying/qualifying_split_2.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw.name
    type = "Block"
    source = "../files/qualifying/qualifying_split_2.json"
}