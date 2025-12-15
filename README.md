# ShopZada 2.0 - Enterprise Data Warehouse Pipeline

A comprehensive data warehouse solution for ShopZada e-commerce platform, built with Apache Airflow, PostgreSQL, and modern ETL best practices.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Pipeline Workflows](#pipeline-workflows)
- [Testing](#testing)
- [Performance Optimizations](#performance-optimizations)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

ShopZada 2.0 is an end-to-end data warehouse pipeline that:

- **Ingests** raw data from 5 departments (Business, Customer, Enterprise, Marketing, Operations)
- **Transforms** data through Staging → ODS → Data Warehouse layers
- **Loads** into a star schema optimized for analytics
- **Validates** data quality at each stage
- **Supports** incremental and full loads

### Key Features

| Feature                 | Description                                    |
| ----------------------- | ---------------------------------------------- |
| **Parallel Processing** | 5 department pipelines run concurrently        |
| **Streaming ETL**       | Operations data processed in optimized batches |
| **Bulk Inserts**        | 10-50x faster using `psycopg2.execute_values`  |
| **Data Quality**        | Automated validation and error handling        |
| **Idempotent Loads**    | Safe to re-run without duplicates              |

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SOURCE DATA (CSV, JSON, Parquet)                │
│  /source_data/{business,customer,enterprise,marketing,operations}/      │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           STAGING LAYER                                  │
│  Raw data with metadata (_ingested_at, _source_file)                    │
│  Tables: staging.{biz,cust,ent,mkt,ops}_*_raw                           │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        ODS LAYER (Operational Data Store)               │
│  Cleaned, validated, typed data                                          │
│  Tables: ods.core_{users,products,merchants,staff,campaigns,orders,...} │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      DATA WAREHOUSE LAYER (Star Schema)                  │
│  Dimensions: dw.dim_{user,product,merchant,staff,campaign,date}         │
│  Facts: dw.fact_{sales,campaign_response}                               │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           ANALYTICS / BI                                 │
│  Dashboards, Reports, Ad-hoc Queries                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Component            | Technology              |
| -------------------- | ----------------------- |
| **Orchestration**    | Apache Airflow 2.x      |
| **Database**         | PostgreSQL 15           |
| **Containerization** | Docker & Docker Compose |
| **ETL Language**     | Python 3.12             |
| **Data Processing**  | Pandas, NumPy           |
| **Testing**          | pytest                  |

## 📊 Data Model

### Dimension Tables

| Table          | Description           | Key Columns                                     |
| -------------- | --------------------- | ----------------------------------------------- |
| `dim_user`     | Customer demographics | user_key, user_id, name, email, location        |
| `dim_product`  | Product catalog       | product_key, product_id, name, category, price  |
| `dim_merchant` | Seller information    | merchant_key, merchant_id, name, type           |
| `dim_staff`    | Staff members         | staff_key, staff_id, name, department           |
| `dim_campaign` | Marketing campaigns   | campaign_key, campaign_id, name, type, discount |
| `dim_date`     | Date dimension        | date_key, full_date, year, quarter, month, day  |

### Fact Tables

| Table                    | Description            | Measures                                            |
| ------------------------ | ---------------------- | --------------------------------------------------- |
| `fact_sales`             | Transaction line items | quantity, unit_price, sale_amount, order_net_amount |
| `fact_campaign_response` | Campaign performance   | availed (boolean), linked to orders                 |

### Entity Relationship Diagram

```
                    ┌──────────────┐
                    │   dim_date   │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│   dim_user   │────│  fact_sales  │────│ dim_product  │
└──────────────┘    └──────┬───────┘    └──────────────┘
                           │
┌──────────────┐           │            ┌──────────────┐
│ dim_merchant │───────────┴────────────│  dim_staff   │
└──────────────┘                        └──────────────┘
                           │
                    ┌──────┴───────┐
                    │ dim_campaign │
                    └──────────────┘
```

## 📁 Project Structure

```
shopzada_3CSD_grp5/
├── docker-compose.yaml          # Docker services configuration
├── Dockerfile                   # Custom Airflow image
├── requirements.txt             # Python dependencies
├── pytest.ini                   # Pytest configuration
│
├── sql/
│   └── init_db.sql              # Database & schema creation
│
├── source/                      # Raw source files
│   ├── business-department/     # Products data
│   ├── customer-management-department/  # Users data (JSON)
│   ├── enterprise-department/   # Merchants, staff, mappings
│   ├── marketing-department/    # Campaigns, transactions
│   └── operations-department/   # Orders, prices, delays
│
├── workflows/                   # Airflow DAGs
│   ├── shopzada_main_dag.py    # Master orchestration DAG
│   ├── stream_operations_pipeline.py  # Optimized operations ETL
│   ├── load_*_staging.py       # Staging loaders (per department)
│   ├── populate_core_*.py      # ODS transformations
│   └── populate_dim_*.py       # DW dimension loaders
│
└── tests/                       # Test suite
    ├── conftest.py             # Pytest fixtures
    ├── test_scenarios.py       # Scenario-based tests
    └── test_data/              # Mock test data files
        ├── scenario1_new_daily_orders/
        ├── scenario2_new_customer_product/
        ├── scenario3_late_campaign/
        └── scenario4_data_quality/
```

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.12+ (for local development)
- 8GB+ RAM recommended

### Installation

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd shopzada_3CSD_grp5
   ```

2. **Start the Docker containers**

   ```bash
   docker-compose up -d
   ```

3. **Wait for Airflow to initialize** (first run takes ~2 minutes)

   ```bash
   docker-compose logs -f airflow-webserver
   ```

4. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

### Running the Pipeline

1. **Enable and trigger the main DAG**

   ```bash
   # Via Airflow UI: Enable "shopzada_main_dag" and click "Trigger DAG"

   # Or via CLI:
   docker-compose exec airflow-webserver airflow dags trigger shopzada_main_dag
   ```

2. **Monitor progress**

   - View DAG runs in Airflow UI
   - Check task logs for detailed progress

3. **Verify data loaded**
   ```bash
   docker-compose exec postgres psql -U airflow -d airflow -c "
     SELECT 'staging' as layer, count(*) FROM staging.ops_orders_raw
     UNION ALL
     SELECT 'ods', count(*) FROM ods.core_orders
     UNION ALL
     SELECT 'dw', count(*) FROM dw.fact_sales;
   "
   ```

## 🔄 Pipeline Workflows

### Main Orchestration DAG

The `shopzada_main_dag` orchestrates all department pipelines:

```
start → test_connection
            │
            ├── Business:    biz_staging → biz_ods → biz_dw → biz_complete
            ├── Customer:    cust_staging → cust_ods → cust_dw → cust_complete
            ├── Enterprise:  ent_staging → ent_ods → ent_dw_* → ent_complete
            ├── Marketing:   mkt_staging → mkt_ods → mkt_dw → mkt_complete
            └── Operations:  ops_staging → ops_streaming → ops_complete
                                                │
                                                ▼
                                         all_complete
                                                │
                                                ▼
                                     fact_campaign_response
                                                │
                                                ▼
                                               end
```

### Department Pipelines

| Department     | Staging DAG             | ODS DAG                    | DW DAG                                    |
| -------------- | ----------------------- | -------------------------- | ----------------------------------------- |
| **Business**   | load_business_staging   | populate_core_products     | populate_dim_product                      |
| **Customer**   | load_customer_staging   | populate_core_users        | populate_dim_user                         |
| **Enterprise** | load_enterprise_staging | populate_core_enterprise   | populate_dim_merchant, populate_dim_staff |
| **Marketing**  | load_marketing_staging  | populate_core_campaigns    | populate_dim_campaign                     |
| **Operations** | load_operations_staging | stream_operations_pipeline | (included in streaming)                   |

### Operations Streaming Pipeline

The operations pipeline is optimized for large data volumes:

```
setup_tracking → load_staging_data → analyze_chunks
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │   Process ODS Chunks  │ (Bulk inserts)
                              └───────────────────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │  Process Fact Tables  │ (Star schema)
                              └───────────────────────┘
                                          │
                                          ▼
                                    verify_completion
```

## 🧪 Testing

### Test Scenarios

The test suite covers 5 key scenarios from the requirements:

| Scenario                       | Description                    | Tests   |
| ------------------------------ | ------------------------------ | ------- |
| **1. New Daily Orders**        | End-to-end pipeline validation | 6 tests |
| **2. New Customer/Product**    | Dimension creation             | 5 tests |
| **3. Late Campaign Data**      | Unknown/late-arriving data     | 5 tests |
| **4. Data Quality**            | Error detection & handling     | 5 tests |
| **5. Performance Aggregation** | Calculation consistency        | 5 tests |

### Running Tests

```bash
# Install test dependencies
pip install pytest pandas numpy

# Run all tests (file validation only, no database required)
pytest tests/test_scenarios.py -v -k "not Database"

# Run specific scenario
pytest tests/test_scenarios.py -v -k "Scenario1"
pytest tests/test_scenarios.py -v -k "Scenario4"

# Run with database integration (requires running PostgreSQL)
pytest tests/test_scenarios.py -v -m db

# Run with coverage
pytest tests/test_scenarios.py -v --cov=workflows
```

### Test Data Files

Located in `tests/test_data/`:

```
scenario1_new_daily_orders/
├── test_orders.csv           # 3 new orders for 2024-12-15
├── test_order_item_prices.csv
├── test_order_item_products.csv
└── test_order_delays.csv

scenario2_new_customer_product/
├── test_orders.csv           # Order with new customer/product
├── test_users.json           # New customer: USR_NEW_TEST_001
├── test_products.csv         # New product: PROD_NEW_TEST_001
└── ...

scenario3_late_campaign/
├── phase1_orders.csv         # Orders without campaign
├── phase2_campaign_data.csv  # Late-arriving campaign data
└── ...

scenario4_data_quality/
├── bad_orders.csv            # Invalid dates, missing IDs
├── bad_prices.csv            # Negative prices, non-numeric
└── ...
```

## ⚡ Performance Optimizations

### Bulk Insert Strategy

| Method             | Speed             | Used In            |
| ------------------ | ----------------- | ------------------ |
| `execute_values()` | **10-50x faster** | ODS & Fact loading |
| `COPY` command     | **20x faster**    | Staging loading    |
| `method='multi'`   | 5x faster         | Fallback option    |

### Configuration Tuning

```python
# Optimized settings in stream_operations_pipeline.py
CHUNK_SIZE = 50000           # Records per batch
BATCH_INSERT_SIZE = 10000    # Rows per execute_values call
SENSOR_POKE_INTERVAL = 2     # Seconds between checks
MAX_ACTIVE_TASKS = 6         # Parallel task limit
```

### Database Optimizations

- **Connection pooling**: `pool_size=5, max_overflow=10`
- **Staging indexes**: Created on `order_id` columns for faster JOINs
- **Constraint deferral**: Disabled during bulk loads
- **Table analysis**: `ANALYZE` after bulk inserts

### Expected Performance

| Operation         | Records | Time            |
| ----------------- | ------- | --------------- |
| Staging Load      | 2M rows | ~2 minutes      |
| ODS Processing    | 2M rows | ~3 minutes      |
| Fact Loading      | 2M rows | ~2 minutes      |
| **Full Pipeline** | 2M rows | **~10 minutes** |

## 🔧 Troubleshooting

### Common Issues

#### 1. DAG not appearing in Airflow UI

```bash
# Check for syntax errors
docker-compose exec airflow-webserver python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import shopzada_main_dag"

# Restart scheduler
docker-compose restart airflow-scheduler
```

#### 2. Database connection errors

```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Test connection
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT 1"
```

#### 3. NULL product_keys in fact_sales

This was fixed by using `ROW_NUMBER()` instead of `raw_index` for matching prices to products. Re-run the pipeline:

```bash
docker-compose exec airflow-webserver airflow dags trigger stream_operations_pipeline
```

#### 4. Empty fact_campaign_response

Ensure all department pipelines complete before the fact table loads:

```bash
# Check pipeline status
docker-compose exec airflow-webserver airflow dags list-runs -d shopzada_main_dag
```

### Viewing Logs

```bash
# Airflow task logs
docker-compose logs -f airflow-worker

# PostgreSQL logs
docker-compose logs -f postgres

# Specific DAG run
docker-compose exec airflow-webserver airflow tasks logs shopzada_main_dag <task_id> <execution_date>
```

### Resetting the Pipeline

```bash
# Clear all DAG runs
docker-compose exec airflow-webserver airflow dags backfill -s 2024-01-01 -e 2024-01-01 --reset-dagruns shopzada_main_dag

# Truncate all tables and reload
docker-compose exec postgres psql -U airflow -d airflow -c "
  TRUNCATE staging.ops_orders_raw CASCADE;
  TRUNCATE ods.core_orders CASCADE;
  TRUNCATE dw.fact_sales CASCADE;
"
```

## 👥 Team

**Group 5 - 3CSD**

## 📄 License

This project is for educational purposes as part of the Data Warehousing course.

---

_Last updated: December 2025_
