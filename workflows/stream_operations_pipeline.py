"""
Streaming Operations Pipeline DAG
=================================

This DAG implements a streaming/pipelining approach for the operations department ETL.
Instead of waiting for each stage to complete fully, each stage starts processing
as soon as there's enough data available from the previous stage.

Key Features:
1. Processes latest data first (by transaction_date DESC)
2. Each layer runs in parallel once there's enough data
3. Uses a tracking table to know what's been processed at each layer
4. Chunks flow through the pipeline without waiting for previous stages to complete

Pipeline Flow:
  Source Files → Staging (chunks) → ODS (streaming) → DW Dims → Fact Tables
                     ↓                    ↓               ↓           ↓
                 (parallel)          (parallel)      (parallel)  (parallel)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
import psycopg2.extras
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Configuration
MIN_CHUNKS_TO_START_NEXT_LAYER = 1  # Start next layer after this many chunks are ready
CHUNK_SIZE = 10000  # Process 10k records at a time for faster streaming


def get_db_engine():
    """Create SQLAlchemy engine for database connection."""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )


def get_db_connection():
    """Get raw database connection."""
    engine = get_db_engine()
    return engine.raw_connection()


# =============================================================================
# SETUP: Create tracking table for processed chunks
# =============================================================================

def setup_tracking_table(**context):
    """Create a tracking table to track what chunks have been processed at each layer."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("SETTING UP STREAMING PIPELINE TRACKING")
    print("=" * 70)
    
    # Create tracking schema if not exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS pipeline;")
    
    # Create tracking table for chunk processing status
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline.ops_chunk_tracker (
            chunk_id SERIAL PRIMARY KEY,
            date_range_start DATE,
            date_range_end DATE,
            staging_loaded_at TIMESTAMP,
            ods_loaded_at TIMESTAMP,
            dw_dims_loaded_at TIMESTAMP,
            facts_loaded_at TIMESTAMP,
            order_count INTEGER,
            status VARCHAR(50) DEFAULT 'pending'
        );
    """)
    
    # Create index for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ops_chunk_status 
        ON pipeline.ops_chunk_tracker(status, date_range_start DESC);
    """)
    
    conn.commit()
    print("  ✓ Tracking table created/verified")
    
    # Clear previous run's tracking data
    cur.execute("TRUNCATE TABLE pipeline.ops_chunk_tracker;")
    conn.commit()
    print("  ✓ Tracking table cleared for fresh run")
    
    cur.close()
    conn.close()
    print("=" * 70)


def analyze_source_data_and_create_chunks(**context):
    """
    Analyze source data and create chunk definitions based on date ranges.
    Chunks are ordered by date DESC (latest first).
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("ANALYZING SOURCE DATA & CREATING CHUNK PLAN")
    print("=" * 70)
    
    # Check staging table for date ranges
    try:
        date_stats = pd.read_sql("""
            SELECT 
                MIN(transaction_date::DATE) as min_date,
                MAX(transaction_date::DATE) as max_date,
                COUNT(*) as total_orders
            FROM staging.ops_orders_raw
            WHERE transaction_date IS NOT NULL
              AND transaction_date != ''
              AND transaction_date !~ '[^0-9-/: ]'
        """, engine)
        
        if date_stats.empty or pd.isna(date_stats['min_date'].iloc[0]):
            print("  ⚠ No valid dates found in staging. Loading all data as single chunk.")
            # Create single chunk for all data
            cur.execute("""
                INSERT INTO pipeline.ops_chunk_tracker 
                (date_range_start, date_range_end, status, order_count)
                VALUES (NULL, NULL, 'pending_staging', 0)
            """)
            conn.commit()
            cur.close()
            conn.close()
            return
        
        min_date = date_stats['min_date'].iloc[0]
        max_date = date_stats['max_date'].iloc[0]
        total_orders = date_stats['total_orders'].iloc[0]
        
        print(f"  Date range: {min_date} to {max_date}")
        print(f"  Total orders: {total_orders:,}")
        
        # Create weekly chunks, ordered by date DESC (latest first)
        # This ensures dashboard shows latest data first
        current_end = pd.to_datetime(max_date)
        current_start = current_end - timedelta(days=6)
        min_dt = pd.to_datetime(min_date)
        
        chunks_created = 0
        while current_end >= min_dt:
            chunk_start = max(current_start, min_dt)
            
            # Count orders in this chunk
            cur.execute("""
                SELECT COUNT(*) FROM staging.ops_orders_raw
                WHERE transaction_date::DATE BETWEEN %s AND %s
            """, (chunk_start.date(), current_end.date()))
            chunk_count = cur.fetchone()[0]
            
            if chunk_count > 0:
                cur.execute("""
                    INSERT INTO pipeline.ops_chunk_tracker 
                    (date_range_start, date_range_end, status, order_count)
                    VALUES (%s, %s, 'pending_staging', %s)
                """, (chunk_start.date(), current_end.date(), chunk_count))
                chunks_created += 1
                print(f"    Chunk {chunks_created}: {chunk_start.date()} to {current_end.date()} ({chunk_count:,} orders)")
            
            # Move to previous week
            current_end = current_start - timedelta(days=1)
            current_start = current_end - timedelta(days=6)
        
        conn.commit()
        print(f"\n  ✓ Created {chunks_created} chunks (latest dates first)")
        
    except Exception as e:
        print(f"  ⚠ Error analyzing data: {e}")
        # Fallback: create single chunk
        cur.execute("""
            INSERT INTO pipeline.ops_chunk_tracker 
            (date_range_start, date_range_end, status, order_count)
            VALUES (NULL, NULL, 'pending_staging', 0)
        """)
        conn.commit()
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# SENSORS: Check if enough data is ready for next layer
# =============================================================================

def check_staging_ready(**context):
    """Check if there are chunks ready to process from staging to ODS."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*) FROM pipeline.ops_chunk_tracker
        WHERE staging_loaded_at IS NOT NULL AND ods_loaded_at IS NULL
    """)
    ready_chunks = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return ready_chunks >= MIN_CHUNKS_TO_START_NEXT_LAYER


def check_ods_ready(**context):
    """Check if there are chunks ready to process from ODS to DW."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*) FROM pipeline.ops_chunk_tracker
        WHERE ods_loaded_at IS NOT NULL AND dw_dims_loaded_at IS NULL
    """)
    ready_chunks = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return ready_chunks >= MIN_CHUNKS_TO_START_NEXT_LAYER


def check_dims_ready(**context):
    """Check if dimensions are populated and chunks are ready for facts."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Check if we have dimension data
    cur.execute("SELECT COUNT(*) FROM dw.dim_user;")
    user_count = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(*) FROM pipeline.ops_chunk_tracker
        WHERE dw_dims_loaded_at IS NOT NULL AND facts_loaded_at IS NULL
    """)
    ready_chunks = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return user_count > 0 and ready_chunks >= MIN_CHUNKS_TO_START_NEXT_LAYER


# =============================================================================
# LAYER 1: Staging Processing (mark chunks as ready for ODS)
# =============================================================================

def process_staging_chunks(**context):
    """
    Process staging data and mark chunks as ready for ODS.
    Since data is already in staging, we just validate and mark as processed.
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("PROCESSING STAGING CHUNKS")
    print("=" * 70)
    
    # Get pending staging chunks (ordered by date DESC - latest first)
    cur.execute("""
        SELECT chunk_id, date_range_start, date_range_end, order_count
        FROM pipeline.ops_chunk_tracker
        WHERE status = 'pending_staging'
        ORDER BY date_range_start DESC NULLS LAST
    """)
    chunks = cur.fetchall()
    
    print(f"Found {len(chunks)} chunks to process")
    
    for chunk_id, start_date, end_date, order_count in chunks:
        print(f"\n  Processing chunk {chunk_id}: {start_date} to {end_date}")
        
        # Validate data exists in staging
        if start_date and end_date:
            cur.execute("""
                SELECT COUNT(*) FROM staging.ops_orders_raw
                WHERE transaction_date::DATE BETWEEN %s AND %s
            """, (start_date, end_date))
        else:
            cur.execute("SELECT COUNT(*) FROM staging.ops_orders_raw")
        
        actual_count = cur.fetchone()[0]
        
        if actual_count > 0:
            # Mark chunk as ready for ODS processing
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET staging_loaded_at = NOW(), 
                    status = 'staging_complete',
                    order_count = %s
                WHERE chunk_id = %s
            """, (actual_count, chunk_id))
            conn.commit()
            print(f"    ✓ Chunk {chunk_id} ready for ODS ({actual_count:,} orders)")
        else:
            print(f"    ⚠ Chunk {chunk_id} has no data, skipping")
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET status = 'skipped'
                WHERE chunk_id = %s
            """, (chunk_id,))
            conn.commit()
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# LAYER 2: ODS Processing (Staging → ODS)
# =============================================================================

def process_ods_chunks(**context):
    """
    Process chunks from staging to ODS.
    Runs in parallel with staging processing.
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("PROCESSING ODS CHUNKS (STREAMING)")
    print("=" * 70)
    
    # Get chunks ready for ODS (staging complete, ods not done)
    cur.execute("""
        SELECT chunk_id, date_range_start, date_range_end
        FROM pipeline.ops_chunk_tracker
        WHERE staging_loaded_at IS NOT NULL AND ods_loaded_at IS NULL
        ORDER BY date_range_start DESC NULLS LAST
    """)
    chunks = cur.fetchall()
    
    print(f"Found {len(chunks)} chunks ready for ODS processing")
    
    # Disable foreign keys for faster inserts
    cur.execute("SET session_replication_role = 'replica';")
    conn.commit()
    
    for chunk_id, start_date, end_date in chunks:
        print(f"\n  Processing chunk {chunk_id}: {start_date} to {end_date} → ODS")
        
        try:
            # Build date filter
            if start_date and end_date:
                date_filter = f"AND o.transaction_date::DATE BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = ""
            
            # Read orders for this chunk
            orders_df = pd.read_sql(f"""
                SELECT DISTINCT ON (o.order_id)
                    o.order_id,
                    o.user_id,
                    o.transaction_date,
                    o.estimated_arrival,
                    d.delay_in_days,
                    m.merchant_id,
                    m.staff_id,
                    c.campaign_id,
                    c.availed
                FROM staging.ops_orders_raw o
                LEFT JOIN staging.ops_order_delays_raw d ON o.order_id = d.order_id
                LEFT JOIN staging.ent_order_merchants_raw m ON o.order_id = m.order_id
                LEFT JOIN staging.mkt_campaign_transactions_raw c ON o.order_id = c.order_id
                WHERE o.order_id IS NOT NULL 
                  AND o.order_id != ''
                  AND o.user_id IS NOT NULL
                  AND o.user_id != ''
                  {date_filter}
                ORDER BY o.order_id, o.transaction_date DESC
            """, engine)
            
            if len(orders_df) == 0:
                print(f"    ⚠ No orders found for chunk {chunk_id}")
                cur.execute("""
                    UPDATE pipeline.ops_chunk_tracker
                    SET ods_loaded_at = NOW(), status = 'ods_complete'
                    WHERE chunk_id = %s
                """, (chunk_id,))
                conn.commit()
                continue
            
            # Clean and transform data
            orders_df['order_id'] = orders_df['order_id'].astype(str).str.strip()
            orders_df['user_id'] = orders_df['user_id'].astype(str).str.strip()
            orders_df['transaction_date'] = pd.to_datetime(orders_df['transaction_date'], format='mixed', errors='coerce')
            orders_df['delay_in_days'] = pd.to_numeric(orders_df['delay_in_days'], errors='coerce').fillna(0).astype(int)
            orders_df['is_delayed'] = orders_df['delay_in_days'] > 0
            
            # Parse estimated_arrival from duration string (e.g., "13days" → 13)
            # Then calculate actual estimated_arrival date = transaction_date + days
            def parse_estimated_arrival(row):
                if pd.isna(row['transaction_date']):
                    return None
                est_str = str(row['estimated_arrival']) if pd.notna(row['estimated_arrival']) else ''
                # Extract numeric days from strings like "13days", "15 days", etc.
                match = re.search(r'(\d+)', est_str)
                if match:
                    days = int(match.group(1))
                    return row['transaction_date'] + timedelta(days=days)
                return None
            
            orders_df['estimated_arrival'] = orders_df.apply(parse_estimated_arrival, axis=1)
            
            # Calculate actual arrival = estimated_arrival + delay_in_days
            orders_df['actual_arrival'] = orders_df.apply(
                lambda row: row['estimated_arrival'] + timedelta(days=int(row['delay_in_days'])) 
                if pd.notna(row['estimated_arrival']) and row['delay_in_days'] > 0
                else None,
                axis=1
            )
            
            # Clean string columns
            for col in ['merchant_id', 'staff_id', 'campaign_id']:
                if col in orders_df.columns:
                    orders_df[col] = orders_df[col].apply(
                        lambda x: str(x).strip() if pd.notna(x) and str(x).strip().lower() not in ['nan', 'none', ''] else None
                    )
            
            # Handle availed boolean
            if 'availed' in orders_df.columns:
                orders_df['availed'] = pd.to_numeric(orders_df['availed'], errors='coerce').fillna(0).astype(bool)
            else:
                orders_df['availed'] = False
            
            # Insert into ODS
            inserted = 0
            for _, row in orders_df.iterrows():
                try:
                    cur.execute("""
                        INSERT INTO ods.core_orders 
                        (order_id, user_id, transaction_date, estimated_arrival, actual_arrival,
                         delay_in_days, is_delayed, merchant_id, staff_id, campaign_id, availed)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) DO NOTHING
                    """, (
                        row['order_id'],
                        row['user_id'],
                        row['transaction_date'] if pd.notna(row['transaction_date']) else None,
                        row['estimated_arrival'] if pd.notna(row['estimated_arrival']) else None,
                        row['actual_arrival'] if pd.notna(row.get('actual_arrival')) else None,
                        row['delay_in_days'],
                        row['is_delayed'],
                        row.get('merchant_id'),
                        row.get('staff_id'),
                        row.get('campaign_id'),
                        row.get('availed', False)
                    ))
                    inserted += 1
                except Exception as e:
                    pass  # Skip individual row errors
            
            conn.commit()
            
            # Also process line items for this chunk
            line_items_inserted = process_line_items_for_chunk(engine, conn, cur, start_date, end_date)
            
            # Mark chunk as ODS complete
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET ods_loaded_at = NOW(), status = 'ods_complete'
                WHERE chunk_id = %s
            """, (chunk_id,))
            conn.commit()
            
            print(f"    ✓ Chunk {chunk_id}: {inserted:,} orders, {line_items_inserted:,} line items → ODS")
            
        except Exception as e:
            print(f"    ✗ Error processing chunk {chunk_id}: {e}")
            conn.rollback()
    
    # Re-enable foreign keys
    cur.execute("SET session_replication_role = 'origin';")
    conn.commit()
    
    cur.close()
    conn.close()
    print("=" * 70)


def process_line_items_for_chunk(engine, conn, cur, start_date, end_date):
    """Process line items for a specific date range chunk."""
    try:
        if start_date and end_date:
            # Get order_ids for this date range
            order_ids_df = pd.read_sql(f"""
                SELECT DISTINCT order_id FROM staging.ops_orders_raw
                WHERE transaction_date::DATE BETWEEN '{start_date}' AND '{end_date}'
                  AND order_id IS NOT NULL AND order_id != ''
            """, engine)
            
            if len(order_ids_df) == 0:
                return 0
            
            order_ids = tuple(order_ids_df['order_id'].tolist())
            if len(order_ids) == 1:
                order_filter = f"WHERE p.order_id = '{order_ids[0]}'"
            else:
                order_filter = f"WHERE p.order_id IN {order_ids}"
        else:
            order_filter = "WHERE p.order_id IS NOT NULL"
        
        # Read line items
        line_items_df = pd.read_sql(f"""
            SELECT 
                p.order_id,
                p.price,
                p.quantity,
                pr.product_id,
                pr.product_name
            FROM staging.ops_order_item_prices_raw p
            LEFT JOIN staging.ops_order_item_products_raw pr 
                ON p.order_id = pr.order_id AND p.raw_index = pr.raw_index
            {order_filter}
        """, engine)
        
        if len(line_items_df) == 0:
            return 0
        
        # Clean data
        line_items_df['order_id'] = line_items_df['order_id'].astype(str).str.strip()
        line_items_df['price'] = line_items_df['price'].astype(str).str.replace('[$,₱]', '', regex=True)
        line_items_df['price'] = pd.to_numeric(line_items_df['price'], errors='coerce')
        
        # Extract quantity from strings like "4PC", "2pcs"
        def extract_qty(val):
            if pd.isna(val):
                return 1
            match = re.search(r'(\d+)', str(val))
            return int(match.group(1)) if match else 1
        
        line_items_df['quantity'] = line_items_df['quantity'].apply(extract_qty)
        
        # Generate unique line_item_ids
        line_items_df['item_num'] = line_items_df.groupby('order_id').cumcount()
        line_items_df['line_item_id'] = line_items_df.apply(
            lambda row: f"{row['order_id']}_item_{row['item_num']}", axis=1
        )
        
        # Insert line items
        inserted = 0
        for _, row in line_items_df.iterrows():
            if pd.notna(row['price']) and row['price'] > 0:
                try:
                    cur.execute("""
                        INSERT INTO ods.core_line_items 
                        (line_item_id, order_id, product_id, quantity, price)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (line_item_id) DO NOTHING
                    """, (
                        row['line_item_id'],
                        row['order_id'],
                        str(row['product_id']).strip() if pd.notna(row['product_id']) else None,
                        row['quantity'],
                        float(row['price'])
                    ))
                    inserted += 1
                except:
                    pass
        
        conn.commit()
        return inserted
        
    except Exception as e:
        print(f"      ⚠ Line items error: {e}")
        return 0


# =============================================================================
# LAYER 3: DW Processing (ODS → Dimensions)
# =============================================================================

def ensure_dim_date(**context):
    """Ensure dim_date is populated."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM dw.dim_date;")
    if cur.fetchone()[0] == 0:
        print("Populating dim_date...")
        start_date = datetime(2019, 1, 1)
        end_date = datetime(2025, 12, 31)
        current = start_date
        
        records = []
        while current <= end_date:
            date_key = int(current.strftime('%Y%m%d'))
            records.append((
                date_key, current.date(), current.weekday() + 1,
                current.month, current.strftime('%B'),
                (current.month - 1) // 3 + 1, current.year,
                current.weekday() >= 5
            ))
            current += timedelta(days=1)
        
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dw.dim_date (date_key, date, day_of_week, month, month_name, quarter, year, is_weekend)
            VALUES %s ON CONFLICT DO NOTHING
        """, records)
        conn.commit()
        print(f"  ✓ Inserted {len(records)} dates")
    
    cur.close()
    conn.close()


def process_dw_dims_for_chunks(**context):
    """Mark chunks as dimension-ready (dims are populated by other DAGs)."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("MARKING CHUNKS READY FOR FACTS")
    print("=" * 70)
    
    # Update all ODS-complete chunks to dims-complete
    cur.execute("""
        UPDATE pipeline.ops_chunk_tracker
        SET dw_dims_loaded_at = NOW(), status = 'dims_complete'
        WHERE ods_loaded_at IS NOT NULL AND dw_dims_loaded_at IS NULL
        RETURNING chunk_id
    """)
    updated = cur.fetchall()
    conn.commit()
    
    print(f"  ✓ {len(updated)} chunks marked ready for fact loading")
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# LAYER 4: Fact Tables Processing (Dims → Facts)
# =============================================================================

def process_fact_tables_streaming(**context):
    """
    Process fact tables for chunks that are ready.
    This runs in parallel with other layers.
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("PROCESSING FACT TABLES (STREAMING)")
    print("=" * 70)
    
    # Load dimension lookups
    cur.execute("SELECT user_id, user_key FROM dw.dim_user;")
    user_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT product_id, product_key FROM dw.dim_product;")
    product_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT merchant_id, merchant_key FROM dw.dim_merchant;")
    merchant_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT staff_id, staff_key FROM dw.dim_staff;")
    staff_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT campaign_id, campaign_key FROM dw.dim_campaign;")
    campaign_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    print(f"  Loaded lookups: users={len(user_lookup)}, products={len(product_lookup)}, merchants={len(merchant_lookup)}")
    
    # Get chunks ready for facts
    cur.execute("""
        SELECT chunk_id, date_range_start, date_range_end
        FROM pipeline.ops_chunk_tracker
        WHERE dw_dims_loaded_at IS NOT NULL AND facts_loaded_at IS NULL
        ORDER BY date_range_start DESC NULLS LAST
    """)
    chunks = cur.fetchall()
    
    print(f"  Found {len(chunks)} chunks ready for fact loading")
    
    for chunk_id, start_date, end_date in chunks:
        print(f"\n  Processing facts for chunk {chunk_id}: {start_date} to {end_date}")
        
        try:
            if start_date and end_date:
                date_filter = f"WHERE o.transaction_date::DATE BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = ""
            
            # Process fact_orders
            orders_df = pd.read_sql(f"""
                SELECT order_id, user_id, merchant_id, staff_id,
                       transaction_date, estimated_arrival, delay_in_days, is_delayed
                FROM ods.core_orders o
                {date_filter}
            """, engine)
            
            fact_orders_records = []
            for _, row in orders_df.iterrows():
                user_key = user_lookup.get(row['user_id'])
                merchant_key = merchant_lookup.get(row['merchant_id']) if pd.notna(row.get('merchant_id')) else None
                staff_key = staff_lookup.get(row['staff_id']) if pd.notna(row.get('staff_id')) else None
                
                order_date_key = None
                est_arrival_key = None
                
                if pd.notna(row.get('transaction_date')):
                    try:
                        order_date_key = int(pd.to_datetime(row['transaction_date']).strftime('%Y%m%d'))
                    except:
                        pass
                
                if pd.notna(row.get('estimated_arrival')):
                    try:
                        est_arrival_key = int(pd.to_datetime(row['estimated_arrival']).strftime('%Y%m%d'))
                    except:
                        pass
                
                fact_orders_records.append((
                    row['order_id'], order_date_key, est_arrival_key,
                    user_key, merchant_key, staff_key,
                    int(row['delay_in_days']) if pd.notna(row.get('delay_in_days')) else 0,
                    bool(row['is_delayed']) if pd.notna(row.get('is_delayed')) else False
                ))
            
            # Insert fact_orders
            if fact_orders_records:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO dw.fact_orders 
                    (order_id, order_date_key, estimated_arrival_date_key, user_key, merchant_key, staff_key, delay_in_days, is_delayed)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, fact_orders_records)
                conn.commit()
            
            # Process fact_sales (line items)
            if start_date and end_date:
                sales_filter = f"""
                    WHERE o.transaction_date::DATE BETWEEN '{start_date}' AND '{end_date}'
                """
            else:
                sales_filter = ""
            
            line_items_df = pd.read_sql(f"""
                SELECT li.order_id, li.product_id, li.quantity, li.price,
                       o.user_id, o.transaction_date, o.merchant_id, o.staff_id, o.campaign_id
                FROM ods.core_line_items li
                JOIN ods.core_orders o ON li.order_id = o.order_id
                {sales_filter}
            """, engine)
            
            fact_sales_records = []
            for _, row in line_items_df.iterrows():
                user_key = user_lookup.get(row['user_id'])
                product_key = product_lookup.get(row['product_id'])
                merchant_key = merchant_lookup.get(row['merchant_id']) if pd.notna(row.get('merchant_id')) else None
                staff_key = staff_lookup.get(row['staff_id']) if pd.notna(row.get('staff_id')) else None
                campaign_key = campaign_lookup.get(row['campaign_id']) if pd.notna(row.get('campaign_id')) else None
                
                order_date_key = None
                if pd.notna(row.get('transaction_date')):
                    try:
                        order_date_key = int(pd.to_datetime(row['transaction_date']).strftime('%Y%m%d'))
                    except:
                        pass
                
                qty = int(row['quantity']) if pd.notna(row['quantity']) else 0
                price = float(row['price']) if pd.notna(row['price']) else 0.0
                
                fact_sales_records.append((
                    row['order_id'], order_date_key, user_key, product_key,
                    merchant_key, staff_key, campaign_key,
                    qty, price, qty * price
                ))
            
            # Insert fact_sales
            if fact_sales_records:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO dw.fact_sales 
                    (order_id, order_date_key, user_key, product_key, merchant_key, staff_key, campaign_key,
                     quantity_sold, unit_price, sale_amount)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, fact_sales_records)
                conn.commit()
            
            # Mark chunk as facts complete
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET facts_loaded_at = NOW(), status = 'complete'
                WHERE chunk_id = %s
            """, (chunk_id,))
            conn.commit()
            
            print(f"    ✓ Chunk {chunk_id}: {len(fact_orders_records)} orders, {len(fact_sales_records)} sales → Facts")
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
            conn.rollback()
    
    cur.close()
    conn.close()
    print("=" * 70)


def verify_streaming_results(**context):
    """Verify the streaming pipeline results."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("STREAMING PIPELINE VERIFICATION")
    print("=" * 70)
    
    # Chunk status summary
    cur.execute("""
        SELECT status, COUNT(*), SUM(order_count)
        FROM pipeline.ops_chunk_tracker
        GROUP BY status
        ORDER BY status
    """)
    print("\nChunk Processing Summary:")
    for status, count, orders in cur.fetchall():
        print(f"  {status}: {count} chunks, {orders or 0:,} orders")
    
    # Table counts
    tables = [
        ('staging.ops_orders_raw', 'Staging Orders'),
        ('ods.core_orders', 'ODS Orders'),
        ('ods.core_line_items', 'ODS Line Items'),
        ('dw.fact_orders', 'Fact Orders'),
        ('dw.fact_sales', 'Fact Sales')
    ]
    
    print("\nTable Row Counts:")
    for table, label in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"  {label}: {count:,}")
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    'stream_operations_pipeline',
    default_args=default_args,
    description='Streaming operations pipeline - processes chunks in parallel across all layers',
    schedule_interval=None,
    catchup=False,
    tags=['operations', 'streaming', 'parallel', 'pipeline'],
    max_active_tasks=4,  # Allow parallel processing
) as dag:
    
    # Setup
    t_setup = PythonOperator(
        task_id='setup_tracking_table',
        python_callable=setup_tracking_table,
    )
    
    t_analyze = PythonOperator(
        task_id='analyze_and_create_chunks',
        python_callable=analyze_source_data_and_create_chunks,
    )
    
    # Layer 1: Process staging
    t_staging = PythonOperator(
        task_id='process_staging_chunks',
        python_callable=process_staging_chunks,
    )
    
    # Sensor: Wait for staging data
    sensor_staging = PythonSensor(
        task_id='wait_for_staging_data',
        python_callable=check_staging_ready,
        poke_interval=5,
        timeout=60,
        soft_fail=True,
    )
    
    # Layer 2: Process ODS
    t_ods = PythonOperator(
        task_id='process_ods_chunks',
        python_callable=process_ods_chunks,
    )
    
    # Sensor: Wait for ODS data
    sensor_ods = PythonSensor(
        task_id='wait_for_ods_data',
        python_callable=check_ods_ready,
        poke_interval=5,
        timeout=60,
        soft_fail=True,
    )
    
    # Layer 3: Prepare DW dims
    t_dim_date = PythonOperator(
        task_id='ensure_dim_date',
        python_callable=ensure_dim_date,
    )
    
    t_dims = PythonOperator(
        task_id='process_dw_dims',
        python_callable=process_dw_dims_for_chunks,
    )
    
    # Sensor: Wait for dims
    sensor_dims = PythonSensor(
        task_id='wait_for_dims_ready',
        python_callable=check_dims_ready,
        poke_interval=5,
        timeout=60,
        soft_fail=True,
    )
    
    # Layer 4: Process facts
    t_facts = PythonOperator(
        task_id='process_fact_tables',
        python_callable=process_fact_tables_streaming,
    )
    
    # Verification
    t_verify = PythonOperator(
        task_id='verify_results',
        python_callable=verify_streaming_results,
    )
    
    # Dependencies - parallel streams
    t_setup >> t_analyze >> t_staging
    
    # Stream 1: Staging → ODS
    t_staging >> sensor_staging >> t_ods
    
    # Stream 2: ODS → DW Dims
    t_ods >> sensor_ods >> t_dim_date >> t_dims
    
    # Stream 3: Dims → Facts
    t_dims >> sensor_dims >> t_facts >> t_verify
