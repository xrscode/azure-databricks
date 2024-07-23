#!/bin/bash

# File to be deleted
FILE_PATH="./terraform/terraform.tfvars"


# Destroy Infrastructure:
cd terraform && terraform destroy -auto-approve

if [ -f "$FILE_PATH" ]; then
    rm "$FILE_PATH"
    echo "$FILE_PATH has been deleted."
    echo "$FILE_PATH infrastructure removed."
else
    echo "$FILE_PATH does not exist."
fi