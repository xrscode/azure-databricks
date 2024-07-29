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
    # depends_on = [azurerm_key_vault.f1keyvault]
}

# Create containers:
# Create Raw container:
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.storage_account_one.name
  container_access_type = "private"
}
resource "azurerm_storage_container" "raw_increment" {
  name                  = "raw-increment"
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


# # 2021-03-21:
# Lap Times
resource "azurerm_storage_blob" "upload-2021-03021-lap_times"{
    for_each = fileset(path.module, "../files/incremental_load/2021-03-21/lap_times/*")
    name = "2021-03-21/lap_times/${replace(each.key, "../files/incremental_load/2021-03-21/lap_times/", "")}"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = each.key
}
# # 2021-03-21:
resource "azurerm_storage_blob" "upload-2021-03021-qualifying"{
    for_each = fileset(path.module, "../files/incremental_load/2021-03-21/qualifying/*")
    name = "2021-03-21/qualifying/${replace(each.key, "../files/incremental_load/2021-03-21/qualifying/", "")}"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = each.key
}
# # 2021-03-21 CIRCUITS:
resource "azurerm_storage_blob" "upload-2021-03021-circuits"{
    name = "2021-03-21/circuits.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-21/circuits.csv"
}
# # 2021-03-21 CONSTRUCTORS:
resource "azurerm_storage_blob" "upload-2021-03021-constructors"{
    name = "2021-03-21/constructors.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-21/constructors.json"
}
# # 2021-03-21 DRIVERS:
resource "azurerm_storage_blob" "upload-2021-03021-drivers"{
    name = "2021-03-21/drivers.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-21/drivers.json"
}
# # 2021-03-21 pit_stops:
resource "azurerm_storage_blob" "upload-2021-03021-pit_stops"{
    name = "2021-03-21/pit_stops.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-21/pit_stops.json"
}
# # 2021-03-21 RACES:
resource "azurerm_storage_blob" "upload-2021-03021-races"{
    name = "2021-03-21/races.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-21/races.csv"
}
# # 2021-03-21 RESULTS:
resource "azurerm_storage_blob" "upload-2021-03021-results"{
    name = "2021-03-21/results.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-21/results.json"
}










# # 2021-03-28:
# Lap Times
resource "azurerm_storage_blob" "upload-2021-03-28-lap_times"{
    for_each = fileset(path.module, "../files/incremental_load/2021-03-28/lap_times/*")
    name = "2021-03-28/lap_times/${replace(each.key, "../files/incremental_load/2021-03-28/lap_times/", "")}"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = each.key
}
resource "azurerm_storage_blob" "upload-2021-03-28-qualifying"{
    for_each = fileset(path.module, "../files/incremental_load/2021-03-28/qualifying/*")
    name = "2021-03-28/qualifying/${replace(each.key, "../files/incremental_load/2021-03-28/qualifying/", "")}"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = each.key
}
# # 2021-03-28 CIRCUITS:
resource "azurerm_storage_blob" "upload-2021-03-28-circuits"{
    name = "2021-03-28/circuits.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-28/circuits.csv"
}
# # 2021-03-28 CONSTRUCTORS:
resource "azurerm_storage_blob" "upload-2021-03-28-constructors"{
    name = "2021-03-28/constructors.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-28/constructors.json"
}
# # 2021-03-28 DRIVERS:
resource "azurerm_storage_blob" "upload-2021-03-28-drivers"{
    name = "2021-03-28/drivers.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-28/drivers.json"
}
# # 2021-03-28 pit_stops:
resource "azurerm_storage_blob" "upload-2021-03-28-pit_stops"{
    name = "2021-03-28/pit_stops.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-28/pit_stops.json"
}
# # 2021-03-28 RACES:
resource "azurerm_storage_blob" "upload-2021-03-28-races"{
    name = "2021-03-28/races.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-28/races.csv"
}
# # 2021-03-28 RESULTS:
resource "azurerm_storage_blob" "upload-2021-03-28-results"{
    name = "2021-03-28/results.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-03-28/results.json"
}





# # 2021-04-18:
# Lap Times
resource "azurerm_storage_blob" "upload-2021-04-18-lap_times"{
    for_each = fileset(path.module, "../files/incremental_load/2021-04-18/lap_times/*")
    name = "2021-04-18/lap_times/${replace(each.key, "../files/incremental_load/2021-04-18/lap_times/", "")}"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = each.key
}
resource "azurerm_storage_blob" "upload-2021-04-18-qualifying"{
    for_each = fileset(path.module, "../files/incremental_load/2021-04-18/qualifying/*")
    name = "2021-04-18/qualifying/${replace(each.key, "../files/incremental_load/2021-04-18/qualifying/", "")}"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = each.key
}
# # 2021-04-18 CIRCUITS:
resource "azurerm_storage_blob" "upload-2021-04-18-circuits"{
    name = "2021-04-18/circuits.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-04-18/circuits.csv"
}
# # 2021-04-18 CONSTRUCTORS:
resource "azurerm_storage_blob" "upload-2021-04-18-constructors"{
    name = "2021-04-18/constructors.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-04-18/constructors.json"
}
# # 2021-04-18 DRIVERS:
resource "azurerm_storage_blob" "upload-2021-04-18-drivers"{
    name = "2021-04-18/drivers.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-04-18/drivers.json"
}
# # 2021-04-18 pit_stops:
resource "azurerm_storage_blob" "upload-2021-04-18-pit_stops"{
    name = "2021-04-18/pit_stops.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-04-18/pit_stops.json"
}
# # 2021-04-18 RACES:
resource "azurerm_storage_blob" "upload-2021-04-18-races"{
    name = "2021-04-18/races.csv"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-04-18/races.csv"
}
# # 2021-04-18 RESULTS:
resource "azurerm_storage_blob" "upload-2021-04-18-results"{
    name = "2021-04-18/results.json"
    storage_account_name = azurerm_storage_account.storage_account_one.name
    storage_container_name = azurerm_storage_container.raw_increment.name
    type = "Block"
    source = "../files/incremental_load/2021-04-18/results.json"
}