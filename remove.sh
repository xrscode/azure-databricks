#!/bin/bash

# File to be deleted
FILE_PATH="./terraform/terraform.tfvars"

# Check if the file exists before attempting to delete it
if [ -f "$FILE_PATH" ]; then
    rm "$FILE_PATH"
    echo "$FILE_PATH has been deleted."
    echo "Removing infrastructure now..."
else
    echo "$FILE_PATH does not exist."
fi

# Destroy Infrastructure:
cd terraform && terraform destroy -auto-approve
