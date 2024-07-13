variable "databricks_token" {
    description = "The Databricks access token."
    type = string
    sensitive = true
}

variable "databricks_host" {
    description = "The Databricks workspace URL."
    type = string
}

variable "databricks_user" {
    description = "The user for the databricks workspace."
    type = string
}

variable "key_vault" {
    description = "The name of the key vault."
    type = string
}