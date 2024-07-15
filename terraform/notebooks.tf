# Formula1 Root directory:
resource "databricks_directory" "formula1" {
    path = "/Users/${var.databricks_user}/Formula1"
}

# set-up sub-directory:
resource "databricks_directory" "setup" {
    path = "${databricks_directory.formula1.path}/set-up"
    depends_on = [databricks_directory.formula1]
}

# Upload notebook; 'access data lake via access keys'.
resource "databricks_notebook" "access_adls_access_keys" {
  content_base64 = filebase64("../src/notebooks/1.access_adls_using_access_keys.py")
  path           = "${databricks_directory.setup.path}/1.access_adls_using_access_keys"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'access data lake via sas token'.
resource "databricks_notebook" "access_adls_sas_token" {
  content_base64 = filebase64("../src/notebooks/2.access_adls_using_sas_token.py")
  path           = "${databricks_directory.setup.path}/1.access_adls_using_access_keys"
  language       = "PYTHON"  # Set the appropriate language
}

# Upload notebook; 'access data lake using service principal'.
resource "databricks_notebook" "access_adls_service_principal" {
  content_base64 = filebase64("../src/notebooks/3.access_adls_using_service_principal.py")
  path           = "${databricks_directory.setup.path}/1.access_adls_using_access_keys"
  language       = "PYTHON"  # Set the appropriate language
}