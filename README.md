# OWID Cloud Data Platform (Azure)

An end-to-end data engineering project that demonstrates ingestion, transformation, and analytics-ready data modeling using **Azure Data Lake Storage Gen2** and **Python**.  
The project implements a full **Medallion Architecture (Bronze → Silver → Gold)** with production-style design choices such as snapshot-based ingestion, data quality enforcement, surrogate keys, star-schema modeling, and Gold-layer aggregations.

This project is intentionally designed to prioritize **correctness, reproducibility, and analytics value** over unnecessary tooling.

---

## 🏗 Architecture Overview
                         +-----------------------------+
                         |   OWID CO2 & GHG Dataset    |
                         |        (Raw CSV)            |
                         +--------------+--------------+
                                        |
                                        |  Web Ingestion
                                        v
        +-------------------------------------------------------------+
        |                 Azure Data Lake Storage Gen2                |
        |                                                             |
        |   +--------------------+    +--------------------+          |
        |   |    BRONZE LAYER    |    |    SILVER LAYER    |          |
        |   |--------------------|    |--------------------|          |
        |   | - Raw snapshots    |--->| - Cleaned data     |          |
        |   | - snapshot_date=   |    | - Schema enforced  |          |
        |   |   YYYY-MM-DD       |    | - Quality checks   |          |
        |   | - CSV format       |    | - Parquet format   |          |
        |   +--------------------+    +--------------------+          |
        |                                     |                       |
        |                                     | Dimensional Modeling  |
        |                                     v                       |
        |                            +--------------------+           |
        |                            |     GOLD LAYER     |           |
        |                            |--------------------|           |
        |                            | - Analytics ready  |           |
        |                            | - Star schema      |           |
        |                            | - Aggregations     |           |
        |                            +----------+---------+           |
        |                                       |                     |
        +---------------------------------------|---------------------+
                                                |
                                                v
                          +-------------------------------------------+
                          |               GOLD DATA MODEL             |
                          |-------------------------------------------|
                          |                                           |
                          |  Dimension Tables:                        |
                          |    - dim_country                          |
                          |                                           |
                          |  Fact Tables:                             |
                          |    - fact_emissions                       |
                          |                                           |
                          |  Aggregations:                            |
                          |    - Global emissions by year             |
                          |    - Top emitting countries               |
                          |    - CO2 emissions per capita             |
                          |                                           |
                          +-------------------------------------------+

The platform follows a layered lakehouse-style architecture:

- **Bronze Layer**  
  Raw OWID CO₂ data ingested directly from the web and stored unchanged using snapshot-based partitioning to guarantee lineage and reproducibility.

- **Silver Layer**  
  Cleaned and standardized data with enforced schemas, data quality checks, and conversion to Parquet for efficient analytics.

- **Gold Layer**  
  Analytics-ready datasets modeled using dimensional design principles, including fact tables, dimension tables, and precomputed aggregations optimized for BI consumption.

All data is stored on **Azure Data Lake Storage Gen2** with hierarchical namespace enabled, and **Parquet** is used as the columnar storage format.

---

## 🧰 Tech Stack

- Python  
- Azure Blob Storage / Azure Data Lake Storage Gen2  
- Pandas  
- PyArrow (Parquet)  
- Requests  
- Git & GitHub  

---

## 📊 Data Source

**Our World in Data (OWID) – CO₂ and Greenhouse Gas Emissions Dataset**

- Public and non-copyrighted  
- Widely used in research, sustainability reporting, and policy analysis  
- Suitable for reproducible data engineering workflows  

---

## 🥉 Bronze Layer

The Bronze layer ingests raw data from the OWID public source without modification.  
Data is stored using **snapshot-based partitioning** (`snapshot_date=YYYY-MM-DD`) to ensure deterministic re-runs, historical traceability, and safe backfills. No transformations or assumptions are applied at this stage.

---

## 🥈 Silver Layer

The Silver layer applies data cleaning and standardization, including:

- Column name normalization (snake_case)
- Data type enforcement
- Removal of invalid or incomplete records
- Fail-fast data quality checks (null validation, year range validation)
- Conversion from CSV to Parquet for columnar storage efficiency

Snapshot handling is parameterized to guarantee consistency between upstream and downstream layers.

---

## 🥇 Gold Layer

The Gold layer exposes analytics-ready datasets following **dimensional modeling best practices**.  
A **star schema** is implemented using surrogate keys to optimize joins and decouple analytics from changing business identifiers.

Gold datasets include:

- **Dimension tables**
  - `dim_country`

- **Fact tables**
  - `fact_emissions` (country × year × emissions metrics)

- **Aggregation tables**
  - Global emissions by year  
  - Top emitting countries per year  
  - CO₂ emissions per capita  

These tables are designed to be consumed directly by BI tools with minimal query complexity.

---

## ⚡ Performance Considerations

Performance optimizations include:

- Selective partitioning of large fact tables by `year`
- Column pruning via Parquet storage
- Precomputed Gold aggregations to reduce query-time computation
- Avoidance of partitioning for small dimension and aggregation tables to limit metadata overhead

---

## ▶️ How to Run

Set the Azure Storage connection string as an environment variable:
AZURE_STORAGE_CONNECTION_STRING=your_connection_string_here

Run Bronze ingestion:
python ingestion/ingest_raw_data.py

Run Silver transformation:
python silver/transform_owid_to_silver.py --snapshot-date YYYY-MM-DD

Build Gold tables and aggregations:
python gold/build_gold_tables.py --snapshot-date YYYY-MM-DD
