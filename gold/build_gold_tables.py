import os
import argparse
import pandas as pd
from azure.storage.blob import BlobServiceClient

# -------------------------------------------------------------------
# 1. Arguments
# -------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Build Gold fact & dimension tables")
parser.add_argument("--snapshot-date", required=True)
args = parser.parse_args()

snapshot_date = args.snapshot_date

# -------------------------------------------------------------------
# 2. Azure connection
# -------------------------------------------------------------------
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not AZURE_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set")

blob_service_client = BlobServiceClient.from_connection_string(
    AZURE_CONNECTION_STRING
)

silver_container = "silver"
gold_container = "gold"

# -------------------------------------------------------------------
# 3. Paths
# -------------------------------------------------------------------
silver_blob_path = (
    f"owid/snapshot_date={snapshot_date}/owid-co2-data.parquet"
)

dim_country_blob = (
    f"dim_country/snapshot_date={snapshot_date}/dim_country.parquet"
)

fact_emissions_blob = (
    f"fact_emissions/snapshot_date={snapshot_date}/fact_emissions.parquet"
)

os.makedirs("data/gold", exist_ok=True)

local_silver = "data/gold/owid_silver.parquet"
local_dim = "data/gold/dim_country.parquet"
local_fact = "data/gold/fact_emissions.parquet"

# -------------------------------------------------------------------
# 4. Read Silver data
# -------------------------------------------------------------------
print("⬇️ Reading Silver data...")

silver_blob_client = blob_service_client.get_blob_client(
    container=silver_container,
    blob=silver_blob_path
)

with open(local_silver, "wb") as f:
    f.write(silver_blob_client.download_blob().readall())

df = pd.read_parquet(local_silver)

# -------------------------------------------------------------------
# 5. Build dimension table with surrogate key
# -------------------------------------------------------------------
print("🧭 Building dim_country with surrogate key...")

dim_columns = ["iso_code", "country"]

if "continent" in df.columns:
    dim_columns.append("continent")

dim_country = (
    df[dim_columns]
    .dropna(subset=["iso_code"])
    .drop_duplicates()
    .sort_values("iso_code")
    .reset_index(drop=True)
)

# Add surrogate key
dim_country["country_sk"] = dim_country.index + 1

# Rename for clarity
dim_country = dim_country.rename(columns={"iso_code": "country_id"})

# -------------------------------------------------------------------
# 6. Build fact table and join surrogate key
# -------------------------------------------------------------------
print("📊 Building fact_emissions with surrogate key...")

fact_columns = [
    col for col in df.columns
    if col not in ["country", "continent"]
]

fact_emissions = df[fact_columns]

# Join surrogate key
fact_emissions = fact_emissions.merge(
    dim_country[["country_sk", "country_id"]],
    left_on="iso_code",
    right_on="country_id",
    how="left"
)

# Drop natural key from fact table
fact_emissions = fact_emissions.drop(
    columns=["iso_code", "country_id"]
)

# Reorder columns (best practice)
cols = ["country_sk", "year"] + [
    c for c in fact_emissions.columns
    if c not in ["country_sk", "year"]
]

fact_emissions = fact_emissions[cols]

# -------------------------------------------------------------------
# 7. Write Gold tables
# -------------------------------------------------------------------
dim_country.to_parquet(local_dim, index=False)
fact_emissions.to_parquet(local_fact, index=False)

print("☁️ Uploading Gold tables...")

blob_service_client.get_blob_client(
    container=gold_container,
    blob=dim_country_blob
).upload_blob(open(local_dim, "rb"), overwrite=True)

blob_service_client.get_blob_client(
    container=gold_container,
    blob=fact_emissions_blob
).upload_blob(open(local_fact, "rb"), overwrite=True)

print("🏆 Gold layer created successfully!")
