# DATABRICKS: Formula1 Root directory:
resource "databricks_directory" "formula1" {
    path = "/Users/${var.databricks_user}/Formula1"
}

# DATABRICKS: Ingestion folder:
resource "databricks_directory" "formula1_ingestion" {
    path = "${databricks_directory.formula1.path}/ingestion"
}

# DATABRICKS: "set-up" folder:
resource "databricks_directory" "setup" {
    path = "${databricks_directory.formula1.path}/set-up"
}

# DATABRICKS: demo folder:
resource "databricks_directory" "demo" {
    path = "${databricks_directory.formula1.path}/demo"
}

# DATABRICKS: transformation folder:
resource "databricks_directory" "transformation" {
    path = "${databricks_directory.formula1.path}/transformation"
}

# SETUP NOTEBOOKS
# Upload notebook; 'access data lake via access keys'.
resource "databricks_notebook" "access_adls_access_keys" {
  content_base64 = filebase64("../src/notebooks/set-up/1.access_adls_using_access_keys.py")
  path           = "${databricks_directory.setup.path}/1.access_adls_using_access_keys"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'access data lake via sas token'.
resource "databricks_notebook" "access_adls_sas_token" {
  content_base64 = filebase64("../src/notebooks/set-up/2.access_adls_using_sas_token.py")
  path           = "${databricks_directory.setup.path}/2.access_adls_using_sas_token"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'access data lake using service principal'.
resource "databricks_notebook" "access_adls_service_principal" {
  content_base64 = filebase64("../src/notebooks/set-up/3.access_adls_using_service_principal.py")
  path           = "${databricks_directory.setup.path}/3.access_adls_using_service_principal"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'access data lake using cluster scoped credentials'
resource "databricks_notebook" "access_adls_cluster_scoped" {
  content_base64 = filebase64("../src/notebooks/set-up/4.access_adls_using_cluster_scoped_credentials.py")
  path           = "${databricks_directory.setup.path}/4.access_adls_using_cluster_scoped_credentials"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'explore dbutils secrets utility'
resource "databricks_notebook" "explore_dbutils_secrets_utility" {
  content_base64 = filebase64("../src/notebooks/set-up/5.explore_dbutils_secrets_utility.py")
  path           = "${databricks_directory.setup.path}/5.explore_dbutils_secrets_utility"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'explore dbfs root'
resource "databricks_notebook" "explore_dbfs_root" {
  content_base64 = filebase64("../src/notebooks/set-up/6.explore_dbfs_root.py")
  path           = "${databricks_directory.setup.path}/6.explore_dbfs_root"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'mount adls using service principle'
resource "databricks_notebook" "mount_adls_service_principle" {
  content_base64 = filebase64("../src/notebooks/set-up/7.mount_adls_using_service_principle.py")
  path           = "${databricks_directory.setup.path}/7.mount_adls_using_service_principle"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'mount_adls_for_project'
resource "databricks_notebook" "mount_adls_for_project" {
  content_base64 = filebase64("../src/notebooks/set-up/8.mount_adls_containers_for_project.py")
  path           = "${databricks_directory.setup.path}/8.mount_adls_containers_for_project"
  language       = "PYTHON"  # Set the appropriate language
}

# INGESTION NOTEBOOKS:
resource "databricks_notebook" "ingest_all_files" {
  content_base64 = filebase64("../src/notebooks/ingestion/0.ingest_all_files.py")
  path           = "${databricks_directory.formula1_ingestion.path}/0.ingest_all_files"
  language       = "PYTHON"  # Set the appropriate language
}


# Upload notebook; 'ingest_circuits_file'
resource "databricks_notebook" "ingest_circuits" {
  content_base64 = filebase64("../src/notebooks/ingestion/1.ingest_circuits_csv.py")
  path           = "${databricks_directory.formula1_ingestion.path}/1.ingest_circuits_csv"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_races_file'
resource "databricks_notebook" "ingest_races" {
  content_base64 = filebase64("../src/notebooks/ingestion/2.ingest_races_csv.py")
  path           = "${databricks_directory.formula1_ingestion.path}/2.ingest_races_csv"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_constructors_file'
resource "databricks_notebook" "ingest_constructors" {
  content_base64 = filebase64("../src/notebooks/ingestion/3.ingest_constructors_csv.py")
  path           = "${databricks_directory.formula1_ingestion.path}/3.ingest_constructors_csv"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_drivers_file'
resource "databricks_notebook" "ingest_drivers" {
  content_base64 = filebase64("../src/notebooks/ingestion/4.ingest_drivers_json.py")
  path           = "${databricks_directory.formula1_ingestion.path}/4.ingest_drivers_json"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_results_json'
resource "databricks_notebook" "ingest_results_json" {
  content_base64 = filebase64("../src/notebooks/ingestion/5.ingest_results_json.py")
  path           = "${databricks_directory.formula1_ingestion.path}/5.ingest_results_json"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_pitstops_json
resource "databricks_notebook" "ingest_pitstops_json" {
  content_base64 = filebase64("../src/notebooks/ingestion/6.ingest_pitstops_json.py")
  path           = "${databricks_directory.formula1_ingestion.path}/6.ingest_pitstops_json"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_lap_times_csv
resource "databricks_notebook" "ingest_lap_times_csv" {
  content_base64 = filebase64("../src/notebooks/ingestion/7.ingest_lap_times_csv.py")
  path           = "${databricks_directory.formula1_ingestion.path}/7.ingest_lap_times_csv"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_qualifying_json
resource "databricks_notebook" "ingest_qualifying_json" {
  content_base64 = filebase64("../src/notebooks/ingestion/8.ingest_qualifying_json.py")
  path           = "${databricks_directory.formula1_ingestion.path}/8.ingest_qualifying_json"
  language       = "PYTHON"  # Set the appropriate language
}

# Create folder 'includes':
resource "databricks_directory" "includes_path" {
    path = "${databricks_directory.formula1.path}/includes"
}

# Upload notebook; 'configuration'
resource "databricks_notebook" "configuration" {
  content_base64 = filebase64("../src/notebooks/includes/configuration.py")
  path           = "${databricks_directory.includes_path.path}/configuration"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'common_functions'
resource "databricks_notebook" "common_functions" {
  content_base64 = filebase64("../src/notebooks/includes/common_functions.py")
  path           = "${databricks_directory.includes_path.path}/common_functions"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'filter_demo'
resource "databricks_notebook" "filter_demo" {
  content_base64 = filebase64("../src/notebooks/demo/1.filter_demo.py")
  path           = "${databricks_directory.demo.path}/1.filter_demo"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'join_demo'
resource "databricks_notebook" "join_demo" {
  content_base64 = filebase64("../src/notebooks/demo/2.join_demo.py")
  path           = "${databricks_directory.demo.path}/2.join_demo"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'outer_join'
resource "databricks_notebook" "outer_join_demo" {
  content_base64 = filebase64("../src/notebooks/demo/3.join_outer_demo.py")
  path           = "${databricks_directory.demo.path}/3.join_outer_demo"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'semi_join'
resource "databricks_notebook" "semi_join_demo" {
  content_base64 = filebase64("../src/notebooks/demo/4.join_semi_demo.py")
  path           = "${databricks_directory.demo.path}/4.join_semi_demo"
  language       = "PYTHON"  # Set the appropriate language
}
# Upload notebook; 'anti_join'
resource "databricks_notebook" "anti_join_demo" {
  content_base64 = filebase64("../src/notebooks/demo/5.join_anti_demo.py")
  path           = "${databricks_directory.demo.path}/5.join_anti_demo"
  language       = "PYTHON"  # Set the appropriate language
}
# Upload notebook; 'cross_join'
resource "databricks_notebook" "cross_join_demo" {
  content_base64 = filebase64("../src/notebooks/demo/6.join_cross_demo.py")
  path           = "${databricks_directory.demo.path}/6.join_cross_demo"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'race_results'
resource "databricks_notebook" "race_results" {
  content_base64 = filebase64("../src/notebooks/transformation/race_results.py")
  path           = "${databricks_directory.transformation.path}/race_results"
  language       = "PYTHON"  # Set the appropriate language
}