# OWID Data Platform (Azure)

End-to-end data engineering project demonstrating
web ingestion, cloud data lake design, and medallion architecture
using Azure Data Lake Gen2.

## Architecture
- Azure Resource Group
- Azure Storage Account (ADLS Gen2)
- Bronze layer with snapshot-based ingestion
- Python ingestion scripts

## Data Source
- Our World in Data (CO₂ and energy dataset)
- Public, scraping-friendly source

## Bronze Layer
- Raw data ingested daily
- Stored using snapshot_date partitioning
- Data remains untouched

## Tech Stack
- Python
- Azure Blob Storage (ADLS Gen2)
- Azure SDK for Python

## Next Steps
- Silver layer (cleaning & typing)
- Data quality checks
- Gold analytics tables
