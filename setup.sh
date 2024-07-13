# Setup initial infrasturcture:
cd terraform && terraform init && terraform plan && terraform apply -auto-approve

# Run Python file to ask for host and token:
cd ../ && python src/update_tfars.py

# Setup databricks:
cd databricks && terraform init && terraform plan && terraform apply -auto-approve