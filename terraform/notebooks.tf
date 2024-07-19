# DATABRICKS: Formula1 Root directory:
resource "databricks_directory" "formula1" {
    path = "/Users/${var.databricks_user}/Formula1"
}

# DATABRICKS: Ingestion sub directory:
resource "databricks_directory" "formula1_ingestion" {
    path = "${databricks_directory.formula1.path}/ingestion"
}

# DATABRICKS: set-up sub-directory:
resource "databricks_directory" "setup" {
    path = "${databricks_directory.formula1.path}/set-up"
    depends_on = [databricks_directory.formula1]
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
  path           = "${databricks_directory.setup.path}/2.access_adls_using_sas_token.py"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'access data lake using service principal'.
resource "databricks_notebook" "access_adls_service_principal" {
  content_base64 = filebase64("../src/notebooks/set-up/3.access_adls_using_service_principal.py")
  path           = "${databricks_directory.setup.path}/3.access_adls_using_service_principal.py"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'access data lake using cluster scoped credentials'
resource "databricks_notebook" "access_adls_cluster_scoped" {
  content_base64 = filebase64("../src/notebooks/set-up/4.access_adls_using_cluster_scoped_credentials.py")
  path           = "${databricks_directory.setup.path}/4.access_adls_using_cluster_scoped_credentials.py"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'explore dbutils secrets utility'
resource "databricks_notebook" "explore_dbutils_secrets_utility" {
  content_base64 = filebase64("../src/notebooks/set-up/5.explore_dbutils_secrets_utility.py")
  path           = "${databricks_directory.setup.path}/5.explore_dbutils_secrets_utility.py"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'explore dbfs root'
resource "databricks_notebook" "explore_dbfs_root" {
  content_base64 = filebase64("../src/notebooks/set-up/6.explore_dbfs_root.py")
  path           = "${databricks_directory.setup.path}/6.explore_dbfs_root.py"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'mount adls using service principle'
resource "databricks_notebook" "mount_adls_service_principle" {
  content_base64 = filebase64("../src/notebooks/set-up/7.mount_adls_using_service_principle.py")
  path           = "${databricks_directory.setup.path}/7.mount_adls_using_service_principle.py"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'mount_adls_for_project'
resource "databricks_notebook" "mount_adls_for_project" {
  content_base64 = filebase64("../src/notebooks/set-up/8.mount_adls_containers_for_project.py")
  path           = "${databricks_directory.setup.path}/8.mount_adls_containers_for_project.py"
  language       = "PYTHON"  # Set the appropriate language
}

# INGESTION NOTEBOOKS:
# Upload notebook; 'ingest_circuits_file'
resource "databricks_notebook" "ingest_circuits" {
  content_base64 = filebase64("../src/notebooks/ingestion/1.ingest_circuits_file.py")
  path           = "${databricks_directory.formula1_ingestion.path}/1.ingest_circuits_file.py"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'ingest_races_file'
resource "databricks_notebook" "ingest_races" {
  content_base64 = filebase64("../src/notebooks/ingestion/2.ingest_races_file.py")
  path           = "${databricks_directory.formula1_ingestion.path}/2.ingest_races_file.py"
  language       = "PYTHON"  # Set the appropriate language
}