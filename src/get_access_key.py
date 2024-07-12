from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Replace with your storage account name
account_name = ""
blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=DefaultAzureCredential()
)

# List the keys
keys = blob_service_client.get_service_properties()
print(keys)
