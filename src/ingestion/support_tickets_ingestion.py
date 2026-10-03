import os
import pandas as pd
import boto3
from io import StringIO
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

db_config = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": os.getenv("MYSQL_PORT", "3306"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE", "careplus_support_db"),
}

S3_BUCKET = "careplus-data-logs"
S3_PREFIX = "support-tickets/raw/"
DATE_TRACKER_FILE = "date_tracker.txt"

AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("SECRET_KEY"),
    "region_name": os.getenv("REGION"),
}

def get_engine(config):
    return create_engine(
        f"mysql+pymysql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )

def upload_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client("s3", **AWS_CONFIG)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Uploaded to s3://{bucket}/{key}")

def read_last_date(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return f.read().strip()
    return "2025-06-30"

def update_last_date(file_path, new_date):
    with open(file_path, "w") as f:
        f.write(new_date)

def get_next_date(last_date_str):
    last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
    return (last_date + timedelta(days=1)).strftime("%Y-%m-%d")

def run_ingestion():
    engine = get_engine(db_config)
    last_date = read_last_date(DATE_TRACKER_FILE)
    next_date = get_next_date(last_date)

    query = f"""
        SELECT * FROM support_tickets
        WHERE DATE(created_at) = '{next_date}';
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print(f"No data found for {next_date}. Skipping upload.")
        return

    s3_key = f"{S3_PREFIX}support_tickets_{next_date}.csv"
    upload_to_s3(df, S3_BUCKET, s3_key)
    update_last_date(DATE_TRACKER_FILE, next_date)
    print(f"Updated tracker to {next_date}")

if __name__ == "__main__":
    run_ingestion()
