# Cluster for DEMO/SETUP
# Create a databricks (single-node) cluster
# 1. Create small cluster:
data "databricks_node_type" "smallest" {
    local_disk = true
}
# 2. Use latest databricks runtime:
data "databricks_spark_version" "latest_lts" {
    long_term_support = true
    depends_on = [ azurerm_databricks_workspace.workspace ]
}
# 3. Create cluster
resource "databricks_cluster" "my_cluster" {
    cluster_name = "f1-cluster"
    node_type_id = data.databricks_node_type.smallest.id
    spark_version = data.databricks_spark_version.latest_lts.id
    autotermination_minutes = 10
    # CONFIGURATION FOR SINGLE NODE CLUSTER!!!!
    num_workers = 0
    spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  } 
    custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

# CLUSTER FOR MAIN PROJECT
# CLUSTER SCOPED AUTHENTICATION (VIA ACCESS KEYS)
# resource "databricks_cluster" "scoped_cluster" {
#     cluster_name = "f1-cluster-scoped-authentication"
#     node_type_id = data.databricks_node_type.smallest.id
#     spark_version = data.databricks_spark_version.latest_lts.id
#     autotermination_minutes = 25
#     # CONFIGURATION FOR SINGLE NODE CLUSTER!!!!
#     num_workers = 0
#     spark_conf = {
#     # Single-node
#     "spark.databricks.cluster.profile" : "singleNode",
#     "spark.master" : "local[*]",
#     "fs.azure.account.key.${azurerm_storage_account.storage_account_one.name}.dfs.core.windows.net" : "{{secrets/f1-scope/storage-account-primary-key}}"
#   } 
#     custom_tags = {
#     "ResourceClass" = "SingleNode"
#   }
# }