# azure-databricks
Repository for building a real world project on Formula 1 Racing using Azure Databricks, Delta Lake, Unity Catalog and Azure Data Factory.

# User Credentials
Set user credentials in terraform.tfvars e.g:
databricks_user = "your_email_here@gmail.com"


# Installation:
<!-- For M1-M2 Macs -->
From root folder, in the terminal type:
run m1


# BUG with Secret Scope:
After infrastructure has been uploaded into the cloud, there is a possible bug with Databricks secret scope.  Secrets can not be accessed in key vault.  To fix the issue;
1. In 'databricks.tf' line 40.  Rename 'f1-scope' to 'f1-scopex'
2. terraform apply -auto-approve
3. Rename 'databricks.tf' line 40 back to 'f1-scope'
4. terraform apply -auto-approve

