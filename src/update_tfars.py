from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

print('Update tfvars running...')
####### DETERMINE HOST URL ########
"""
The purpose of this file is to write the necessary variables in '../databricks/terraform.tfvars'.
The host_url is read from './terraform/provider.tf'.  This is used to determine the key vault url. 

After the key vault url has been determined, the databricks_host_url secret is retrieved.

Next, the function; 'update_tfvars(h)' is run.  It will prompt the user for:
1. Username; e.g. 'xrs@icloud.com'
2. Databricks Personal Access Token. 
The personal access token will need to be manually retrieved from the databricks workspace. 
"""

print('Reading vault key...')
with open('./terraform/provider.tf', 'r') as f:
    provider_doc = f.readlines()
    # vault_url = provider_doc[30][31:].strip()
    for i, line in enumerate(provider_doc):
        if line.strip() == 'resource "azurerm_key_vault" "f1keyvault" {':
            vault_url = provider_doc[i+1][31:].strip()[1:-1]
            break

# Key Vault URL:
key_vault_url = f"https://{vault_url}.vault.azure.net/"
# Initialise DefaultAzureCredential
credential = DefaultAzureCredential()
# Initalise SecretClient
client = SecretClient(vault_url=key_vault_url, credential=credential)
# Name of secret to retrieve:
databricks_host_url = "databricks-url"
# Retrieve secret:
print('Accessing secret...')
retrieve_host = client.get_secret(databricks_host_url).value
print('Secret acquired!')


def update_tfvars(h):
    # TOKEN:
    print('Databricks personal access token will need to be acquired manually')
    print('Please type token:')
    token = input()
    print(f"'{token}' provided thank you.")

    # USER:
    print('Please type user id:')
    user = input()
    print(f"'{user}' provided thank you.")
    with open('./databricks/terraform.tfvars', 'w') as f:
        f.write(f"""databricks_user = "{user}" \ndatabricks_host = "{h}" \ndatabricks_token = "{token}" \nkey_vault = "{vault_url}"
        """)
    return 'tfvars updated.'


update_tfvars(retrieve_host)
