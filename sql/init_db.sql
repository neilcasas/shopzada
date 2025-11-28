CREATE DATABASE shopzada;

\c shopzada;

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.core_users (
    user_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    gender VARCHAR(50),
    birthdate DATE,
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    device_address VARCHAR(255),
    creation_date TIMESTAMP,
    user_type VARCHAR(50),
    job_title VARCHAR(255), -- Merged from user_jobs
    job_level VARCHAR(50),  -- Merged from user_jobs
    issuing_bank VARCHAR(100),
    credit_card_token VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS ods.core_products (
    product_id VARCHAR(255) PRIMARY KEY,
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS ods.core_merchants (
    merchant_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ods.core_staff (
    staff_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    job_level VARCHAR(50),
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ods.core_campaigns (
    campaign_id VARCHAR(255) PRIMARY KEY,
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS ods.core_orders (
    order_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) REFERENCES ods.core_users(user_id),
    merchant_id VARCHAR(255) REFERENCES ods.core_merchants(merchant_id),
    staff_id VARCHAR(255) REFERENCES ods.core_staff(staff_id),
    campaign_id VARCHAR(255) REFERENCES ods.core_campaigns(campaign_id),
    transaction_date TIMESTAMP,
    estimated_arrival TIMESTAMP,
    actual_arrival TIMESTAMP, 
    delay_in_days INTEGER, 
    is_delayed BOOLEAN,   
    availed BOOLEAN
);

CREATE TABLE IF NOT EXISTS ods.core_line_items (
    line_item_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) REFERENCES ods.core_orders(order_id),
    product_id VARCHAR(255) REFERENCES ods.core_products(product_id),
    quantity INTEGER,
    price DECIMAL(10,2) -- Snapshot of price at time of purchase
);


CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_user (
    user_key SERIAL PRIMARY KEY, -- Surrogate Key
    user_id VARCHAR(255),        -- Business Key
    name VARCHAR(255),
    gender VARCHAR(50),
    birthdate DATE,
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    user_type VARCHAR(50),
    job_title VARCHAR(255),
    job_level VARCHAR(50)
    -- Note: Sensitive Bank Info Excluded
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(255),
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS dw.dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_id VARCHAR(255),
    name VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dw.dim_staff (
    staff_key SERIAL PRIMARY KEY,
    staff_id VARCHAR(255),
    name VARCHAR(255),
    job_level VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dw.dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR(255),
    campaign_name VARCHAR(255),
    discount DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key INTEGER PRIMARY KEY, -- Format: YYYYMMDD
    date DATE,
    day_of_week INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

-- --- Fact Tables ---

-- Fact Sales: Grain = One Line Item
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id VARCHAR(255),               -- Degenerate Dimension
    order_date_key INTEGER REFERENCES dw.dim_date(date_key),
    user_key INTEGER REFERENCES dw.dim_user(user_key),
    product_key INTEGER REFERENCES dw.dim_product(product_key),
    merchant_key INTEGER REFERENCES dw.dim_merchant(merchant_key),
    staff_key INTEGER REFERENCES dw.dim_staff(staff_key),
    campaign_key INTEGER REFERENCES dw.dim_campaign(campaign_key),
    
    quantity_sold INTEGER,
    unit_price DECIMAL(10,2),
    sale_amount DECIMAL(10,2)            -- (qty * unit_price)
);

-- Fact Orders: Grain = One Order (Logistics & Delays)
CREATE TABLE IF NOT EXISTS dw.fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(255),               -- Degenerate Dimension
    order_date_key INTEGER REFERENCES dw.dim_date(date_key),
    estimated_arrival_date_key INTEGER REFERENCES dw.dim_date(date_key),
    
    user_key INTEGER REFERENCES dw.dim_user(user_key),
    merchant_key INTEGER REFERENCES dw.dim_merchant(merchant_key),
    staff_key INTEGER REFERENCES dw.dim_staff(staff_key),
    
    delay_in_days INTEGER,
    is_delayed BOOLEAN
);

-- Fact Campaign Response: Grain = One Order w/ Attribution
CREATE TABLE IF NOT EXISTS dw.fact_campaign_response (
    response_key SERIAL PRIMARY KEY,
    order_id VARCHAR(255),
    transaction_date_key INTEGER REFERENCES dw.dim_date(date_key),
    
    user_key INTEGER REFERENCES dw.dim_user(user_key),
    merchant_key INTEGER REFERENCES dw.dim_merchant(merchant_key),
    campaign_key INTEGER REFERENCES dw.dim_campaign(campaign_key),
    
    order_total_amount DECIMAL(10,2),
    order_discount_amount DECIMAL(10,2),
    order_net_amount DECIMAL(10,2)
);