from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import hashlib
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_db_engine():
    """Create SQLAlchemy engine for database connection."""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )

def extract_from_staging(**context):
    """Extract customer data from staging tables."""
    engine = get_db_engine()
    
    print("=" * 70)
    print("EXTRACTING DATA FROM STAGING SCHEMA")
    print("=" * 70)
    
    # Extract from staging tables
    try:
        # User profiles
        print("Extracting user profiles...")
        user_profiles = pd.read_sql(
            "SELECT * FROM staging.cust_user_profiles_raw",
            engine
        )
        print(f"  ✓ Loaded {len(user_profiles)} user profiles")
        
        # User jobs
        print("Extracting user jobs...")
        user_jobs = pd.read_sql(
            "SELECT * FROM staging.cust_user_jobs_raw",
            engine
        )
        print(f"  ✓ Loaded {len(user_jobs)} user jobs")
        
        # Credit cards
        print("Extracting credit cards...")
        credit_cards = pd.read_sql(
            "SELECT * FROM staging.cust_credit_cards_raw",
            engine
        )
        print(f"  ✓ Loaded {len(credit_cards)} credit cards")
        
        # Store in XCom
        context['ti'].xcom_push(key='user_profiles', value=user_profiles.to_json(orient='split'))
        context['ti'].xcom_push(key='user_jobs', value=user_jobs.to_json(orient='split'))
        context['ti'].xcom_push(key='credit_cards', value=credit_cards.to_json(orient='split'))
        
        print("\n✓ All staging data extracted successfully")
        
    except Exception as e:
        print(f"✗ Error extracting from staging: {str(e)}")
        raise

def transform_and_clean(**context):
    """Transform and clean data from staging."""
    ti = context['ti']
    
    print("=" * 70)
    print("TRANSFORMING AND CLEANING DATA")
    print("=" * 70)
    
    # Retrieve data from XCom
    user_profiles = pd.read_json(ti.xcom_pull(key='user_profiles', task_ids='extract_from_staging'), orient='split')
    user_jobs = pd.read_json(ti.xcom_pull(key='user_jobs', task_ids='extract_from_staging'), orient='split')
    credit_cards = pd.read_json(ti.xcom_pull(key='credit_cards', task_ids='extract_from_staging'), orient='split')
    
    print(f"User profiles shape: {user_profiles.shape}")
    print(f"User jobs shape: {user_jobs.shape}")
    print(f"Credit cards shape: {credit_cards.shape}")
    
    # Ensure user_id is string for merging
    for df in [user_profiles, user_jobs, credit_cards]:
        if 'user_id' in df.columns:
            df['user_id'] = df['user_id'].astype(str)
    
    # Remove duplicates
    user_profiles = user_profiles.drop_duplicates(subset=['user_id'], keep='first')
    user_jobs = user_jobs.drop_duplicates(subset=['user_id'], keep='first')
    credit_cards = credit_cards.drop_duplicates(subset=['user_id'], keep='first')
    
    print(f"After deduplication - Profiles: {len(user_profiles)}, Jobs: {len(user_jobs)}, Cards: {len(credit_cards)}")
    
    # Merge datasets
    print("\nMerging datasets...")
    merged = user_profiles.copy()
    
    # Merge job data
    job_cols = [col for col in user_jobs.columns if col in ['user_id', 'job_title', 'job_level']]
    if len(job_cols) > 1:
        merged = merged.merge(
            user_jobs[job_cols],
            on='user_id',
            how='left',
            suffixes=('', '_job')
        )
        print(f"  ✓ Merged job data ({len(job_cols)} columns)")
    
    # Tokenize credit card numbers and merge
    if 'credit_card_number' in credit_cards.columns:
        print("  → Tokenizing credit card numbers...")
        credit_cards['credit_card_token'] = credit_cards['credit_card_number'].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) and str(x).lower() != 'nan' else None
        )
        credit_cards = credit_cards.drop(columns=['credit_card_number'])
    
    cc_cols = [col for col in credit_cards.columns if col in ['user_id', 'issuing_bank', 'credit_card_token']]
    if len(cc_cols) > 1:
        merged = merged.merge(
            credit_cards[cc_cols],
            on='user_id',
            how='left',
            suffixes=('', '_cc')
        )
        print(f"  ✓ Merged credit card data ({len(cc_cols)} columns)")
    
    print(f"\nMerged data shape: {merged.shape}")
    
    # Clean anomalies
    print("\nCleaning data...")
    
    # Remove records with missing critical fields
    initial_count = len(merged)
    merged = merged[merged['user_id'].notna()]
    if 'name' in merged.columns:
        merged = merged[merged['name'].notna()]
    print(f"  → Removed {initial_count - len(merged)} records with missing critical fields")
    
    # Standardize gender values
    if 'gender' in merged.columns:
        merged['gender'] = merged['gender'].astype(str).str.strip().str.title()
        merged['gender'] = merged['gender'].replace('Nan', None)
        valid_genders = ['Male', 'Female', 'Other', 'Non-Binary', 'Prefer Not To Say']
        merged.loc[~merged['gender'].isin(valid_genders), 'gender'] = None
    
    # Fix birthdate anomalies
    if 'birthdate' in merged.columns:
        merged['birthdate'] = pd.to_datetime(merged['birthdate'], errors='coerce')
        merged.loc[merged['birthdate'] > datetime.now(), 'birthdate'] = None
        merged.loc[merged['birthdate'] < pd.Timestamp('1900-01-01'), 'birthdate'] = None
    
    # Fix creation_date anomalies
    if 'creation_date' in merged.columns:
        merged['creation_date'] = pd.to_datetime(merged['creation_date'], errors='coerce')
        merged.loc[merged['creation_date'] > datetime.now(), 'creation_date'] = None
    
    # Standardize user_type
    if 'user_type' in merged.columns:
        merged['user_type'] = merged['user_type'].astype(str).str.strip().str.title()
        merged['user_type'] = merged['user_type'].replace('Nan', None)
    
    # Handle Students (no job level)
    if 'job_title' in merged.columns and 'job_level' in merged.columns:
        student_mask = merged['job_title'] == 'Student'
        merged.loc[student_mask, 'job_level'] = 'N/A'
    
    # Remove any remaining duplicates
    merged = merged.drop_duplicates(subset=['user_id'], keep='first')
    
    # Drop metadata columns from staging
    metadata_cols = ['_source_file', '_ingested_at', 'raw_index']
    merged = merged.drop(columns=[col for col in metadata_cols if col in merged.columns], errors='ignore')
    
    print(f"\n✓ Final cleaned data shape: {merged.shape}")
    print(f"  Columns: {merged.columns.tolist()}")
    
    # Store in XCom
    context['ti'].xcom_push(key='cleaned_data', value=merged.to_json(orient='split', date_format='iso'))

def load_to_ods(**context):
    """Load cleaned data into ods.core_users table."""
    ti = context['ti']
    engine = get_db_engine()
    
    print("=" * 70)
    print("LOADING DATA TO ODS.CORE_USERS")
    print("=" * 70)
    
    # Retrieve cleaned data
    cleaned_data = pd.read_json(
        ti.xcom_pull(key='cleaned_data', task_ids='transform_and_clean'),
        orient='split'
    )
    
    print(f"Loading {len(cleaned_data)} records to ods.core_users")
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    # Truncate table first
    print("Truncating ods.core_users table...")
    cursor.execute("TRUNCATE TABLE ods.core_users CASCADE;")
    conn.commit()
    
    # Get the columns available in the table
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'ods' AND table_name = 'core_users'
        ORDER BY ordinal_position;
    """)
    db_columns = [row[0] for row in cursor.fetchall()]
    print(f"Database columns: {db_columns}")
    
    # Map dataframe columns to database columns
    available_columns = [col for col in db_columns if col in cleaned_data.columns]
    print(f"Columns to insert: {available_columns}")
    
    # Insert data
    insert_count = 0
    error_count = 0
    
    for _, row in cleaned_data.iterrows():
        try:
            # Build dynamic INSERT statement
            placeholders = ', '.join(['%s'] * len(available_columns))
            columns_str = ', '.join(available_columns)
            
            values = tuple(row.get(col) for col in available_columns)
            
            cursor.execute(
                f"INSERT INTO ods.core_users ({columns_str}) VALUES ({placeholders})",
                values
            )
            insert_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only print first 5 errors
                print(f"  ✗ Error inserting row {row.get('user_id')}: {str(e)}")
    
    conn.commit()
    
    # Verify the load
    cursor.execute("SELECT COUNT(*) FROM ods.core_users;")
    final_count = cursor.fetchone()[0]
    
    print(f"\n✓ Successfully inserted {insert_count} records")
    if error_count > 0:
        print(f"  ⚠ Failed inserts: {error_count}")
    print(f"  Final count in ods.core_users: {final_count}")
    
    cursor.close()
    conn.close()
    
    print("=" * 70)

with DAG(
    'populate_core_users',
    default_args=default_args,
    description='Extract customer data from staging, transform, and load to ods.core_users',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'ods', 'customer', 'staging-to-ods'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_from_staging',
        python_callable=extract_from_staging,
    )
    
    transform_task = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_and_clean,
    )
    
    load_task = PythonOperator(
        task_id='load_to_ods',
        python_callable=load_to_ods,
    )
    
    # Set dependencies
    extract_task >> transform_task >> load_task

    # Set dependencies
    extract_task >> transform_task >> load_task
