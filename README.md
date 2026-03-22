# Azure End-to-End Data Engineering Pipeline
### Medallion Lakehouse Architecture | AdventureWorksLT2017

![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?style=for-the-badge&logo=microsoftazure)
![PySpark](https://img.shields.io/badge/PySpark-Databricks-E25A1C?style=for-the-badge&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-003366?style=for-the-badge)
![Power BI](https://img.shields.io/badge/Power-BI-F2C811?style=for-the-badge&logo=powerbi)

---

## Project Overview

A production-style end-to-end Azure data engineering pipeline that ingests data from an on-premises SQL Server, transforms it through a medallion lakehouse architecture (Bronze → Silver → Gold), and serves it to Power BI dashboards via Azure Synapse Analytics.

---

## Architecture

```
On-Prem SQL Server (AdventureWorksLT2017)
            │
            │  Self-Hosted Integration Runtime (SHIR)
            ▼
  Azure Data Factory (ADF)
  Lookup → ForEach → Copy Activity
            │
            │  Parquet format
            ▼
  ADLS Gen2 ── Bronze Layer (Raw)
            │
            │  PySpark · Databricks
            ▼
  ADLS Gen2 ── Silver Layer (Cleaned)
            │
            │  PySpark · Aggregations · Delta
            ▼
  ADLS Gen2 ── Gold Layer (Aggregated)
            │
            │  Serverless SQL Pool
            ▼
  Azure Synapse Analytics
            │
            ▼
       Power BI Dashboards
            
  Azure Key Vault ──► Secures all credentials
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Source** | SQL Server · AdventureWorksLT2017 |
| **Ingestion** | Azure Data Factory · SHIR |
| **Storage** | Azure Data Lake Storage Gen2 |
| **Transformation** | Azure Databricks · PySpark · Delta Lake |
| **Serving** | Azure Synapse Analytics · Serverless SQL |
| **Visualization** | Power BI |
| **Security** | Azure Key Vault · Microsoft Entra ID |
| **Authentication** | Service Principal · OAuth 2.0 |

---

## Dataset

**AdventureWorksLT2017** — Microsoft sample database with 10 tables:

| Table | Rows | Description |
|---|---|---|
| `SalesOrderHeader` | 32 | Order transactions |
| `SalesOrderDetail` | 542 | Order line items |
| `Customer` | 847 | Customer master |
| `Product` | 295 | Product catalog |
| `ProductCategory` | 41 | Product hierarchy |
| `ProductDescription` | 762 | Product descriptions |
| `ProductModel` | 128 | Product models |
| `ProductModelProductDescription` | 762 | Model-description mapping |
| `Address` | 450 | Address master |
| `CustomerAddress` | 417 | Customer-address mapping |

---

## Pipeline Breakdown

### 1. Ingestion — Bronze Layer
- ADF pipeline dynamically ingests **all 10 tables** using **Lookup + ForEach + Copy** pattern
- Self-Hosted Integration Runtime (SHIR) connects on-premises SQL Server to ADF
- Raw data stored in **Parquet format** in ADLS Gen2 Bronze container
- All credentials managed via **Azure Key Vault**

### 2. Transformation — Silver Layer
PySpark transformations applied to all 10 tables:
- Cast data types (string → int, double, timestamp → date)
- Trim whitespace on all string columns
- Drop duplicates on primary keys
- Filter null primary keys
- Drop sensitive columns (PasswordHash, PasswordSalt, rowguid)
- Add derived columns (IsDiscontinued, DiscountedPrice, OrderStatus label)
- Written as **Delta format** with `overwriteSchema=true`

### 3. Aggregation — Gold Layer
6 aggregated Delta tables built for BI consumption:

| Gold Table | Description |
|---|---|
| `Sales_By_Customer` | Revenue, order count, avg order value per customer |
| `Sales_By_Product` | Revenue, qty sold, avg price per product |
| `Sales_By_Category` | Revenue and orders per product category |
| `Monthly_Sales_Trend` | Revenue and order trends by month/year |
| `Sales_By_Geography` | Revenue by city, state, country |
| `Product_Performance` | Product catalog with category and model details |

---

## Security Architecture

```
Azure Key Vault
├── entra-client-id       → Entra ID App Client ID
├── entra-tenant-id       → Entra ID Tenant ID
├── entra-client-secret   → Entra ID Client Secret
├── adls-storage-key      → ADLS Gen2 Access Key
├── databricks-token      → Databricks PAT Token
├── synapse-sql-password  → Synapse SQL Admin Password
└── sql-server-password   → On-Prem SQL Server Password
```

- **Entra ID Service Principal** used for Databricks → ADLS Gen2 authentication
- **OAuth 2.0** used for all Azure service-to-service communication
- **RBAC** roles assigned following least-privilege principle

---

## Repository Structure

```
azure-de-project/
│
├── notebooks/
│   ├── 01_set_configs.py
│   ├── 02_bronze_to_silver.py
│   └── 03_silver_to_gold.py
│
├── adf_pipeline/
│   └── pl_master_pipeline.json
│
├── synapse/
│   └── create_external_tables.sql
│
├── powerbi/
│   └── dashboard_screenshot.png
│
├── architecture/
│   └── architecture_diagram.png
│
└── README.md
```

---

## How to Run

### Prerequisites
- Azure Subscription (Pay-As-You-Go)
- SQL Server with AdventureWorksLT2017 restored
- Azure resources created in West US 3:
  - ADLS Gen2, ADF, Databricks, Synapse, Key Vault

### Steps
1. **Restore** AdventureWorksLT2017 on local SQL Server
2. **Configure** Key Vault secrets
3. **Set up** Entra ID App Registration and assign Storage Blob Data Contributor role
4. **Run** ADF pipeline to load Bronze layer
5. **Run** Databricks notebooks in order:
   - `01_set_configs`
   - `02_bronze_to_silver`
   - `03_silver_to_gold`
6. **Create** Synapse external tables pointing to Gold layer
7. **Connect** Power BI to Synapse

---

## Key Learnings

- Implementing **medallion lakehouse architecture** on Azure
- Dynamic pipeline design with **ADF Lookup + ForEach** pattern
- **Delta Lake** schema enforcement and ACID transactions
- **Entra ID OAuth** authentication for secure service-to-service access
- **Azure Key Vault** integration across multiple Azure services
- PySpark data transformations at scale in **Databricks**
- Serving data via **Synapse Serverless SQL Pool** to Power BI

---

## Author
**Aishwariya**
Data Analytics Engineering
