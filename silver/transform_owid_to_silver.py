import os
import argparse
import pandas as pd
from azure.storage.blob import BlobServiceClient

# -------------------------------------------------------------------
# 1. Parse arguments
# -------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Transform OWID Bronze → Silver")
parser.add_argument(
    "--snapshot-date",
    required=False,
    help="Snapshot date to process (YYYY-MM-DD). Defaults to latest available."
)
args = parser.parse_args()

# -------------------------------------------------------------------
# 2. Azure connection
# -------------------------------------------------------------------
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not AZURE_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set")

blob_service_client = BlobServiceClient.from_connection_string(
    AZURE_CONNECTION_STRING
)

bronze_container = "bronze"
silver_container = "silver"

# -------------------------------------------------------------------
# 3. Resolve snapshot date
# -------------------------------------------------------------------
owid_prefix = "owid/"

container_client = blob_service_client.get_container_client(bronze_container)

snapshots = set()

for blob in container_client.list_blobs(name_starts_with=owid_prefix):
    parts = blob.name.split("/")
    for part in parts:
        if part.startswith("snapshot_date="):
            snapshots.add(part.replace("snapshot_date=", ""))

if not snapshots:
    raise RuntimeError("No Bronze snapshots found")

latest_snapshot = sorted(snapshots)[-1]

snapshot_date = args.snapshot_date or latest_snapshot

print(f"📅 Using snapshot_date={snapshot_date}")

# -------------------------------------------------------------------
# 4. Paths
# -------------------------------------------------------------------
bronze_blob_path = (
    f"owid/snapshot_date={snapshot_date}/owid-co2-data.csv"
)

silver_blob_path = (
    f"owid/snapshot_date={snapshot_date}/owid-co2-data.parquet"
)

os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/silver", exist_ok=True)

local_raw = "data/raw/owid-co2-data.csv"
local_silver = "data/silver/owid-co2-data.parquet"

# -------------------------------------------------------------------
# 5. Download Bronze data
# -------------------------------------------------------------------
print("⬇️ Downloading Bronze data...")

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
# 6. Silver transformations
# -------------------------------------------------------------------
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

df = df.dropna(subset=["country", "year"])

# -------------------------------------------------------------------
# 7. Write Silver Parquet
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
