Project Staging Layer Architecture1. Schema StrategyWe will use a single schema staging with department prefixes. This keeps all raw data in one place but logically separated.CREATE SCHEMA IF NOT EXISTS staging; 2. Table Definitions (By Department)Crucial Design Rule: All columns are defined as TEXT or VARCHAR.Why? To safely handle mixed formats (JSON, HTML, etc.) and dirty data without crashing the load job.Metadata: We add _source_file to every table to track exactly which file (e.g., the 2020 vs 2023 order file) the row came from.A. Business Department (biz_)Source: product_list.xlsxCREATE TABLE staging.biz_products_raw (
raw_index VARCHAR, -- Handles "Unnamed: 0"
product_id VARCHAR,
product_name VARCHAR,
product_type VARCHAR,
price VARCHAR,

    _ingested_at    TIMESTAMP DEFAULT NOW(),
    _source_file    VARCHAR

);
B. Customer Management (cust\_)Sources: user_credit_card.pickle, user_data.json, user_job.csv-- Source: user_credit_card.pickle
CREATE TABLE staging.cust_credit_cards_raw (
user_id VARCHAR,
name VARCHAR,
credit_card_number VARCHAR,
issuing_bank VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Source: user_data.json
CREATE TABLE staging.cust_user_profiles_raw (
user_id VARCHAR,
name VARCHAR,
gender VARCHAR,
birthdate VARCHAR,
street VARCHAR,
city VARCHAR,
state VARCHAR,
country VARCHAR,
device_address VARCHAR,
creation_date VARCHAR,
user_type VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Source: user*job.csv
CREATE TABLE staging.cust_user_jobs_raw (
raw_index VARCHAR,
user_id VARCHAR,
name VARCHAR,
job_title VARCHAR,
job_level VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);
C. Enterprise Department (ent*)Sources: merchant_data.html, staff_data.html, and order_with_merchant (Parquet/CSV)-- Source: merchant_data.html
CREATE TABLE staging.ent_merchants_raw (
raw_index VARCHAR,
merchant_id VARCHAR,
creation_date VARCHAR,
name VARCHAR,
street VARCHAR,
state VARCHAR,
city VARCHAR,
country VARCHAR,
contact_number VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Source: order_with_merchant_data1, 2, 3 (Unified Table)
CREATE TABLE staging.ent_order_merchants_raw (
raw_index VARCHAR, -- CSV has this, Parquet might not (insert NULL)
order_id VARCHAR,
merchant_id VARCHAR,
staff_id VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Source: staff*data.html
CREATE TABLE staging.ent_staff_raw (
raw_index VARCHAR,
staff_id VARCHAR,
name VARCHAR,
job_level VARCHAR,
street VARCHAR,
state VARCHAR,
city VARCHAR,
country VARCHAR,
contact_number VARCHAR,
creation_date VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);
D. Marketing Department (mkt*)Sources: campaign_data.csv, transactional_campaign_data.csv-- Source: campaign_data.csv
CREATE TABLE staging.mkt_campaigns_raw (
raw_index VARCHAR,
campaign_id VARCHAR,
campaign_name VARCHAR,
campaign_description VARCHAR,
discount VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Source: transactional*campaign_data.csv
CREATE TABLE staging.mkt_campaign_transactions_raw (
raw_index VARCHAR,
transaction_date VARCHAR,
campaign_id VARCHAR,
order_id VARCHAR,
estimated_arrival VARCHAR,
availed VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);
E. Operations Department (ops*)Sources: Mixed formats (Parquet, Pickle, CSV, JSON, HTML)This is the most complex section. We group files by Logic, not by format.-- Group 1: Prices (line_item_data_prices 1, 2, 3)
CREATE TABLE staging.ops_order_item_prices_raw (
raw_index VARCHAR,
order_id VARCHAR,
price VARCHAR,
quantity VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Group 2: Products (line_item_data_products 1, 2, 3)
CREATE TABLE staging.ops_order_item_products_raw (
raw_index VARCHAR,
order_id VARCHAR,
product_name VARCHAR,
product_id VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Group 3: Orders (order_data 2020-2024 across 5 different file formats)
-- We unify them all into ONE staging table.
CREATE TABLE staging.ops_orders_raw (
raw_index VARCHAR, -- CSV/HTML/JSON have indices, Parquet might not
order_id VARCHAR,
user_id VARCHAR,
estimated_arrival VARCHAR,
transaction_date VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
);

-- Group 4: Delays
CREATE TABLE staging.ops_order_delays_raw (
raw_index VARCHAR,
order_id VARCHAR,
delay_in_days VARCHAR,
\_ingested_at TIMESTAMP DEFAULT NOW(),
\_source_file VARCHAR
); 3. Airflow Implementation StrategySince you cannot run COPY commands on .pickle or .parquet files directly in Postgres, your Airflow DAGs must follow this pattern:Pattern for Standard CSVsDirect Load: File -> Postgres COPY (Fastest)Pattern for Exotic Formats (Pickle, Parquet, JSON, HTML)Pandas Intermediate: File -> Pandas DataFrame -> Postgres INSERTExample Airflow Task for Pickle/Parquet:def load_exotic_file(filename, table_name):
import pandas as pd
from sqlalchemy import create_engine

    # 1. Detect format and read
    if filename.endswith('.pickle'):
        df = pd.read_pickle(filename)
    elif filename.endswith('.parquet'):
        df = pd.read_parquet(filename)
    elif filename.endswith('.html'):
        df = pd.read_html(filename)[0] # HTML returns a list of tables

    # 2. Normalize columns (Ensure everything is string)
    df = df.astype(str)

    # 3. Add Metadata
    df['_source_file'] = filename
    df['_ingested_at'] = pd.Timestamp.now()

    # 4. Insert to Staging
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    # Using 'append' allows multiple files (e.g., 2020 orders, 2021 orders) to go into one table
    df.to_sql(table_name, engine, schema='staging', if_exists='append', index=False)

4. Final Data FlowExtract: Airflow reads inputs like user_credit_card.pickle (using Pandas) and campaign_data.csv (using Copy).Load: All data lands in staging.\* tables as text strings.Transform: SQL tasks join these staging tables to create clean schemas (e.g., joining staging.ops_order_item_prices_raw with staging.ops_order_item_products_raw to make a final fact_order_items table).
