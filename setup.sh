# Loginto Azure and create terraform.tfvars:
python ./src/azure_login.py

# Setup initial infrasturcture:
cd terraform && terraform init && terraform plan && terraform apply -auto-approve