# 🏗️ FMCG Incremental Data Pipeline

A production-grade **Bronze → Silver → Gold** medallion architecture pipeline built on **Databricks** and **Delta Lake**, processing FMCG data across customers, products, prices, and orders — from raw CSV ingestion to a clean, analytics-ready data model merged into a unified parent company warehouse.

---

## 📌 Project Overview

This pipeline processes master and transactional data for the **Sports Bar** platform (an FMCG brand) across four domains: **Customers, Products, Prices, and Orders**. Each domain flows through the Bronze → Silver → Gold medallion layers. The final Gold tables are upserted into the parent company's unified data model, making Sports Bar data visible in company-wide analytics and reporting.

---

## 🗂️ Repository Structure

```
consolidated_pipeline/
├── 1_setup/
│   └── utilities                        # Shared schema variables (bronze/silver/gold)
├── 1_customers_data_processing.ipynb    # Dimension: Customers
├── 2_products_data_processing.ipynb     # Dimension: Products
├── 3_pricing_data_processing.ipynb      # Dimension: Prices
├── 4_incremental_load_fact.ipynb        # Fact: Orders (incremental)
└── README.md
```

---

## 🔁 Databricks Job — FMCG INCREMENTAL UPDATE

All four notebooks are orchestrated as a **single Databricks Workflow**, running sequentially:

```
dim_processing_customers
        ↓
dim_processing_products
        ↓
dim_processing_prices
        ↓
fact_processing_orders
```

Dimensions are processed first to ensure referential integrity before the fact table loads. The fact notebook uses **incremental loading** — only new or changed records are processed on each run using Delta Lake's Change Data Feed.

---

## 🏛️ Architecture: Medallion Layers

```
         S3 Raw Files (CSV)
               ↓
    ┌─────────────────────┐
    │   🥉 Bronze Layer   │  Raw ingestion, no transformations
    │                     │  + read_timestamp, file_name, file_size
    └─────────┬───────────┘
              ↓
    ┌─────────────────────┐
    │   🥈 Silver Layer   │  Data quality fixes, enrichment,
    │                     │  type casting, standardisation
    └─────────┬───────────┘
              ↓
    ┌─────────────────────┐
    │   🥇 Gold Layer     │  Business-ready dimension & fact tables
    │                     │  sb_dim_customers, sb_dim_products,
    │                     │  sb_dim_prices, sb_fact_orders
    └─────────┬───────────┘
              ↓
    ┌─────────────────────┐
    │  🔀 Delta Merge     │  Upsert into parent company tables
    │                     │  fmcg.gold.dim_customers, dim_products,
    │                     │  dim_prices, fact_orders
    └─────────────────────┘
```

---

## ⚙️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Databricks** | Notebook execution & workflow orchestration |
| **Apache Spark (PySpark)** | Distributed data processing |
| **Delta Lake** | ACID transactions, time travel, Change Data Feed, upserts |
| **AWS S3** | Raw data source |
| **Unity Catalog** | Three-part table naming (`catalog.schema.table`) |
| **Databricks Workflows** | Sequential job orchestration with dependency management |

---

## 📓 Notebook Details

### 1. `dim_processing_customers` ✅
Processes the customer master dimension.

**Silver Transformations:**
| # | Fix | Detail |
|---|-----|--------|
| 1 | Drop duplicates | Deduplicate on `customer_id` (39 → 35 rows) |
| 2 | Trim whitespace | Strip spaces from `customer_name` |
| 3 | Fix city typos | Dictionary correction + whitelist (`Bengaluru`, `Hyderabad`, `New Delhi`) |
| 4 | Standardise casing | `initcap` on `customer_name` |
| 5 | Fill missing cities | Business-confirmed lookup join for 4 null-city customers |
| 6 | Cast `customer_id` | `integer` → `string` |

**Added columns:** `customer` (composite key), `market`, `platform`, `channel`
**Gold table:** `fmcg.gold.sb_dim_customers`
**Merged into:** `fmcg.gold.dim_customers`

---

### 2. `dim_processing_products` ✅
Processes the product master dimension.

**Pipeline:** Bronze ingest from S3 → Silver quality fixes → Gold selection → Merge into `fmcg.gold.dim_products`

---

### 3. `dim_processing_prices` ✅
Processes the pricing dimension.

**Pipeline:** Bronze ingest from S3 → Silver quality fixes → Gold selection → Merge into `fmcg.gold.dim_prices`

---

### 4. `fact_processing_orders` ✅ *(Incremental)*
Processes the orders fact table using **incremental loading** via Delta Lake's Change Data Feed. Only records that are new or changed since the last run are processed — making each pipeline execution fast and cost-efficient regardless of total data volume.

**Pipeline:** Read CDF changes from Silver dims → Join with dimension keys → Append/merge into `fmcg.gold.fact_orders`

---

## 🚀 How to Run

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to `s3://sportsbar-final/` (or update `base_path` per notebook)
- Databricks Runtime 10.0+ (Delta Lake included)
- The `utilities` notebook deployed at `/Workspace/consolidated_pipeline/1_setup/utilities`

### Option A — Run the Full Workflow (Recommended)

1. Go to **Workflows** in your Databricks workspace
2. Open the **FMCG INCREMENTAL UPDATE** job
3. Click **Run Now**

All four notebooks execute in sequence automatically.

### Option B — Run Individual Notebooks

1. Open any notebook in Databricks
2. Attach to a cluster
3. Set widget values:
   - **Catalog**: `fmcg`
   - **Data Source**: `customers` / `products` / `prices` / `orders`
4. Click **Run All**

### Widget Parameters

| Widget | Default | Description |
|--------|---------|-------------|
| `catalog` | `fmcg` | Unity Catalog name |
| `data_source` | varies | Source folder name and table suffix |

---

## 📊 Data Quality Summary (Customers)

| Issue | Rows Affected | Fix Applied |
|-------|--------------|-------------|
| Duplicate `customer_id` | 4 duplicates | `dropDuplicates` |
| Whitespace in names | 6 rows | `F.trim()` |
| City typos | 7 variants | Dictionary mapping + whitelist |
| Inconsistent casing | Multiple | `F.initcap()` |
| Missing city | 4 rows | Business-confirmed lookup join |
| Wrong ID type | All rows | `.cast("string")` |

---

## 🔑 Key Delta Lake Features Used

| Feature | Where Used |
|---------|-----------|
| **Change Data Feed** | All Bronze & Silver writes; fact incremental reads |
| **Schema evolution** (`mergeSchema`) | Silver writes when new columns are added |
| **ACID overwrite** | Bronze and Gold full refreshes |
| **Delta merge (upsert)** | All Gold → parent table integrations |
| **Time travel** | Full version history for audit and recovery |

---

## 📐 Data Model

```
                    fmcg.gold.dim_customers
                           │
fmcg.gold.dim_products ────┤
                           ├──── fmcg.gold.fact_orders
fmcg.gold.dim_prices ──────┘
```

The fact table references all three dimension tables via their respective keys, forming a standard **star schema** for FMCG analytics.

---

## 📚 Reference

- Built following the [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) pattern

---

## 👤 Author

**Ghanshyam13** — [GitHub Profile](https://github.com/Ghanshyam13)
