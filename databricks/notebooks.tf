provider "databricks" {
  host = var.databricks_host
  token = var.databricks_token
}

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.0.0"
    }
  }
}

# Formula1 Root directory:
resource "databricks_directory" "formula1" {
    path = "/Users/${var.databricks_user}/Formula1"
}

# set-up sub-directory:
resource "databricks_directory" "setup" {
    path = "${databricks_directory.formula1.path}/set-up"
    depends_on = [databricks_directory.formula1]
}

# ADLS ACCESS KEYS NOTEBOOK
resource "databricks_notebook" "access_adls_access_keys" {
  content_base64 = base64encode(data.template_file.adls_notebook.rendered)
  path           = "${databricks_directory.setup.path}/1.access_adls_using_access_keys"
  language       = "PYTHON"
}

# Upload ADLS notebook:
data "template_file" "adls_notebook" {
    template = file("../src/notebooks/1_access_adsl_using_access_keys.py")
}