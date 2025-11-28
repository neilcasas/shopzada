from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="debug_s3_connection",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # 'None' means it only runs when you click Play
    catchup=False,
    tags=["debug", "shopzada"],
) as dag:

    @task
    def list_s3_files():
        # 1. Initialize the Hook using the connection ID you made in the UI
        hook = S3Hook(aws_conn_id="aws_default")
        bucket_name = "shopzada-3csd-grp5"

        # 2. Check if bucket exists
        exists = hook.check_for_bucket(bucket_name)
        if not exists:
            raise ValueError(f"Bucket '{bucket_name}' not found or permission denied.")

        print(f"✅ Connection Successful! Bucket '{bucket_name}' found.")

        # 3. List actual files
        keys = hook.list_keys(bucket_name=bucket_name)
        if keys:
            print(f"Found {len(keys)} files:")
            for key in keys:
                print(f" - {key}")
        else:
            print("Bucket is empty, but connection works!")

    # 4. Execute the task
    list_s3_files()
