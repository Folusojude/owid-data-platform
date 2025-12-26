import os
from datetime import date
import requests
from azure.storage.blob import BlobServiceClient

# -------------------------------------------------------------------
# 1. Read Azure connection string from environment variable
# -------------------------------------------------------------------
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

if not AZURE_CONNECTION_STRING:
    raise ValueError(
        "AZURE_STORAGE_CONNECTION_STRING environment variable is not set"
    )

# -------------------------------------------------------------------
# 2. Define snapshot date (Bronze layer is snapshot-based)
# -------------------------------------------------------------------
snapshot_date = date.today().isoformat()

# -------------------------------------------------------------------
# 3. Azure Blob Storage settings
# -------------------------------------------------------------------
container_name = "bronze"
blob_path = f"owid/snapshot_date={snapshot_date}/owid-co2-data.csv"

# -------------------------------------------------------------------
# 4. Local paths
# -------------------------------------------------------------------
os.makedirs("data/raw", exist_ok=True)
local_file = "data/raw/owid-co2-data.csv"

# -------------------------------------------------------------------
# 5. Reliable, scraping-friendly dataset (Our World in Data)
# -------------------------------------------------------------------
url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"

# -------------------------------------------------------------------
# 6. Download dataset
# -------------------------------------------------------------------
print("⬇️ Downloading OWID dataset...")
response = requests.get(url, timeout=60)
response.raise_for_status()

with open(local_file, "wb") as f:
    f.write(response.content)

print("✅ Download complete")

# -------------------------------------------------------------------
# 7. Upload to Azure Blob Storage (Bronze layer)
# -------------------------------------------------------------------
print("☁️ Uploading to Azure Blob Storage...")

blob_service_client = BlobServiceClient.from_connection_string(
    AZURE_CONNECTION_STRING
)

blob_client = blob_service_client.get_blob_client(
    container=container_name,
    blob=blob_path
)

with open(local_file, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print("🎉 OWID dataset uploaded successfully!")
