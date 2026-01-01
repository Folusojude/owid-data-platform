import os
import pandas as pd
from datetime import date
from azure.storage.blob import BlobServiceClient

# -------------------------------------------------------------------
# 1. Azure connection
# -------------------------------------------------------------------
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not AZURE_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set")

blob_service_client = BlobServiceClient.from_connection_string(
    AZURE_CONNECTION_STRING
)

# -------------------------------------------------------------------
# 2. Paths
# -------------------------------------------------------------------
snapshot_date = "2025-12-26"

bronze_container = "bronze"
silver_container = "silver"

bronze_blob_path = (
    f"owid/snapshot_date={snapshot_date}/owid-co2-data.csv"
)

silver_blob_path = (
    f"owid/snapshot_date={snapshot_date}/owid-co2-data.parquet"
)

local_raw = "data/raw/owid-co2-data.csv"
local_silver = "data/silver/owid-co2-data.parquet"

os.makedirs("data/silver", exist_ok=True)

# -------------------------------------------------------------------
# 3. Download Bronze data
# -------------------------------------------------------------------
print("⬇️ Reading Bronze data from Azure...")

bronze_blob_client = blob_service_client.get_blob_client(
    container=bronze_container,
    blob=bronze_blob_path
)

with open(local_raw, "wb") as f:
    f.write(bronze_blob_client.download_blob().readall())

df = pd.read_csv(local_raw)

print(f"Rows: {len(df):,}")
print(f"Columns: {len(df.columns)}")

# -------------------------------------------------------------------
# 4. Basic cleaning (Silver rules)
# -------------------------------------------------------------------

# Standardize column names
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

# Ensure year is integer
df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

# Drop rows with no country or year
df = df.dropna(subset=["country", "year"])

# -------------------------------------------------------------------
# 5. Write Silver data (Parquet)
# -------------------------------------------------------------------
df.to_parquet(local_silver, index=False)

print("☁️ Uploading Silver dataset...")

silver_blob_client = blob_service_client.get_blob_client(
    container=silver_container,
    blob=silver_blob_path
)

with open(local_silver, "rb") as data:
    silver_blob_client.upload_blob(data, overwrite=True)

print("🎉 Silver layer written successfully!")
