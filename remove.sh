#!/bin/bash

# Destroy Databricks Notebooks:
cd databricks && terraform destroy -auto-approve

# Destroy Infrastructure:
cd ../terraform && terraform destroy -auto-approve
