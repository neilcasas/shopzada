from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import pickle
import hashlib
import os
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_source_path():
    """Get the correct path to source files."""
    base_paths = [
        '/opt/airflow/source/customer-management-department',
        '/home/vgr/dev/school/shopzada_3CSD_grp5/source/customer-management-department'
    ]
    
    for path in base_paths:
        if os.path.exists(path):
            print(f"Using source path: {path}")
            return path
    
    raise FileNotFoundError(f"Could not find source files in any of: {base_paths}")

def extract_user_data(**context):
    """Extract user data from JSON file."""
    source_path = get_source_path()
    user_data_path = os.path.join(source_path, 'user_data.json')
    
    print(f"Reading user data from: {user_data_path}")
    
    # Read JSON (column-oriented format)
    with open(user_data_path, 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    print(f"Loaded {len(df)} records from user_data.json")
    print(f"Columns: {df.columns.tolist()}")
    
    # Store in XCom
    context['ti'].xcom_push(key='user_data', value=df.to_json(orient='records'))

def extract_user_jobs(**context):
    """Extract user job data from CSV file."""
    source_path = get_source_path()
    user_job_path = os.path.join(source_path, 'user_job.csv')
    
    print(f"Reading user job data from: {user_job_path}")
    
    df = pd.read_csv(user_job_path, index_col=0)
    
    print(f"Loaded {len(df)} records from user_job.csv")
    print(f"Columns: {df.columns.tolist()}")
    
    # Store in XCom
    context['ti'].xcom_push(key='user_jobs', value=df.to_json(orient='records'))

def extract_credit_cards(**context):
    """Extract credit card data from pickle file and tokenize."""
    source_path = get_source_path()
    cc_path = os.path.join(source_path, 'user_credit_card.pickle')
    
    print(f"Reading credit card data from: {cc_path}")
    
    with open(cc_path, 'rb') as f:
        df = pickle.load(f)
    
    print(f"Loaded {len(df)} records from user_credit_card.pickle")
    print(f"Columns: {df.columns.tolist()}")
    
    # Tokenize credit card numbers (SHA256 hash)
    if 'credit_card_number' in df.columns:
        df['credit_card_token'] = df['credit_card_number'].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) else None
        )
        # Drop the raw credit card number
        df = df.drop(columns=['credit_card_number'])
        print("Credit card numbers tokenized with SHA256")
    
    # Store in XCom
    context['ti'].xcom_push(key='credit_cards', value=df.to_json(orient='records'))

def clean_and_merge_data(**context):
    """Clean data and merge all sources."""
    ti = context['ti']
    
    # Retrieve data from XCom
    user_data = pd.read_json(ti.xcom_pull(key='user_data', task_ids='extract_user_data'))
    user_jobs = pd.read_json(ti.xcom_pull(key='user_jobs', task_ids='extract_user_jobs'))
    credit_cards = pd.read_json(ti.xcom_pull(key='credit_cards', task_ids='extract_credit_cards'))
    
    print(f"User data shape: {user_data.shape}")
    print(f"User jobs shape: {user_jobs.shape}")
    print(f"Credit cards shape: {credit_cards.shape}")
    
    # Remove duplicates
    user_data = user_data.drop_duplicates(subset=['user_id'])
    user_jobs = user_jobs.drop_duplicates(subset=['user_id'])
    credit_cards = credit_cards.drop_duplicates(subset=['user_id'])
    
    print(f"After deduplication - User data: {len(user_data)}, Jobs: {len(user_jobs)}, Cards: {len(credit_cards)}")
    
    # Merge datasets
    merged = user_data.merge(user_jobs[['user_id', 'job_title', 'job_level']], 
                             on='user_id', 
                             how='left')
    
    merged = merged.merge(credit_cards[['user_id', 'issuing_bank', 'credit_card_token']], 
                         on='user_id', 
                         how='left')
    
    print(f"Merged data shape: {merged.shape}")
    
    # Clean anomalies
    print("Cleaning anomalies...")
    
    # 1. Remove records with missing critical fields
    initial_count = len(merged)
    merged = merged[merged['user_id'].notna()]
    merged = merged[merged['name'].notna()]
    print(f"Removed {initial_count - len(merged)} records with missing user_id or name")
    
    # 2. Standardize gender values
    if 'gender' in merged.columns:
        merged['gender'] = merged['gender'].str.strip().str.title()
        valid_genders = ['Male', 'Female', 'Other', 'Non-Binary', 'Prefer Not To Say']
        merged.loc[~merged['gender'].isin(valid_genders), 'gender'] = None
    
    # 3. Fix birthdate anomalies
    if 'birthdate' in merged.columns:
        merged['birthdate'] = pd.to_datetime(merged['birthdate'], errors='coerce')
        # Remove future birthdates
        merged.loc[merged['birthdate'] > datetime.now(), 'birthdate'] = None
        # Remove unrealistic birthdates (before 1900)
        merged.loc[merged['birthdate'] < pd.Timestamp('1900-01-01'), 'birthdate'] = None
    
    # 4. Fix creation_date anomalies
    if 'creation_date' in merged.columns:
        merged['creation_date'] = pd.to_datetime(merged['creation_date'], errors='coerce')
        # Remove future dates
        merged.loc[merged['creation_date'] > datetime.now(), 'creation_date'] = None
    
    # 5. Standardize user_type
    if 'user_type' in merged.columns:
        merged['user_type'] = merged['user_type'].str.strip().str.title()
    
    # 6. Handle Students (no job level)
    if 'job_title' in merged.columns and 'job_level' in merged.columns:
        student_mask = merged['job_title'] == 'Student'
        merged.loc[student_mask, 'job_level'] = 'N/A'
    
    # 7. Remove duplicate rows based on user_id (keep first)
    merged = merged.drop_duplicates(subset=['user_id'], keep='first')
    
    print(f"Final cleaned data shape: {merged.shape}")
    print(f"Columns: {merged.columns.tolist()}")
    
    # Store in XCom
    context['ti'].xcom_push(key='cleaned_data', value=merged.to_json(orient='records', date_format='iso'))

def load_to_postgres(**context):
    """Load cleaned data into ods.core_users table."""
    ti = context['ti']
    
    # Retrieve cleaned data
    cleaned_data = pd.read_json(ti.xcom_pull(key='cleaned_data', task_ids='clean_and_merge'))
    
    print(f"Loading {len(cleaned_data)} records to PostgreSQL")
    
    # Database connection
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database='shopzada',
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow')
    )
    
    cursor = conn.cursor()
    
    # Truncate table first
    print("Truncating ods.core_users table...")
    cursor.execute("TRUNCATE TABLE ods.core_users CASCADE;")
    
    # Insert data
    insert_count = 0
    error_count = 0
    
    for _, row in cleaned_data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO ods.core_users (
                    user_id, name, gender, birthdate, street, city, state, country,
                    device_address, creation_date, user_type, job_title, job_level,
                    issuing_bank, credit_card_token
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get('user_id'),
                row.get('name'),
                row.get('gender'),
                row.get('birthdate'),
                row.get('street'),
                row.get('city'),
                row.get('state'),
                row.get('country'),
                row.get('device_address'),
                row.get('creation_date'),
                row.get('user_type'),
                row.get('job_title'),
                row.get('job_level'),
                row.get('issuing_bank'),
                row.get('credit_card_token')
            ))
            insert_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only print first 5 errors
                print(f"Error inserting row {row.get('user_id')}: {str(e)}")
    
    conn.commit()
    
    # Verify the load
    cursor.execute("SELECT COUNT(*) FROM ods.core_users;")
    final_count = cursor.fetchone()[0]
    
    print(f"Successfully inserted {insert_count} records")
    print(f"Failed inserts: {error_count}")
    print(f"Final count in ods.core_users: {final_count}")
    
    cursor.close()
    conn.close()

with DAG(
    'populate_core_users',
    default_args=default_args,
    description='Extract customer management data, clean anomalies, and load to ods.core_users',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'ods', 'customer'],
) as dag:
    
    extract_user_data_task = PythonOperator(
        task_id='extract_user_data',
        python_callable=extract_user_data,
    )
    
    extract_user_jobs_task = PythonOperator(
        task_id='extract_user_jobs',
        python_callable=extract_user_jobs,
    )
    
    extract_credit_cards_task = PythonOperator(
        task_id='extract_credit_cards',
        python_callable=extract_credit_cards,
    )
    
    clean_and_merge_task = PythonOperator(
        task_id='clean_and_merge',
        python_callable=clean_and_merge_data,
    )
    
    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )
    
    # Set dependencies
    [extract_user_data_task, extract_user_jobs_task, extract_credit_cards_task] >> clean_and_merge_task >> load_to_postgres_task
