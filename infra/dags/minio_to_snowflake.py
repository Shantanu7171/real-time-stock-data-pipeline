import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY =os.getenv("MINIO_SECRET_KEY")

BUCKET = "bronze-transactions"
LOCAL_DIR = "/opt/airflow/downloads"  # use absolute path for Airflow


SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def download_from_minio():

    print("Starting MinIO download...")

    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    print("Connected to MinIO")

    response = s3.list_objects_v2(Bucket=BUCKET)

    print(f"Response: {response}")

    objects = response.get("Contents", [])

    print(f"Objects found: {len(objects)}")

    local_files = []

    for obj in objects:

        key = obj["Key"]

        local_file = os.path.join(
            LOCAL_DIR,
            os.path.basename(key)
        )

        s3.download_file(
            BUCKET,
            key,
            local_file
        )

        print(f"Downloaded {key} -> {local_file}")

        local_files.append(local_file)

    if not local_files:
        print("No files found in MinIO.")

    return local_files


def load_to_snowflake(**kwargs):

    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')

    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
         user=SNOWFLAKE_USER,
         password=SNOWFLAKE_PASSWORD,
         account=SNOWFLAKE_ACCOUNT,
         warehouse=SNOWFLAKE_WAREHOUSE,
         database=SNOWFLAKE_DB,
         schema=SNOWFLAKE_SCHEMA
    )

    cur = conn.cursor()

    # 1. Upload JSON files to Snowflake internal table stage
    print("Uploading files to Snowflake...")

    cur.execute("""
        PUT 'file:///opt/airflow/downloads/*.json'
        @%BRONZE_STOKES_QUOTES_RAW
        AUTO_COMPRESS=FALSE
        OVERWRITE=FALSE
    """)

    print("PUT result:")
    print(cur.fetchall())

    # 2. Load files from stage into table
    print("Loading data into Snowflake table...")

    cur.execute("""
        COPY INTO BRONZE_STOKES_QUOTES_RAW
        FROM @%BRONZE_STOKES_QUOTES_RAW
        FILE_FORMAT = (
            TYPE = JSON
        )
    """)

    print("COPY result:")
    print(cur.fetchall())

    cur.close()
    conn.close()

    print("Data loaded successfully.")

    
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2
