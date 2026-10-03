import os
import boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

LOGS_FOLDER = "day-wise-logs-data/"
DATE_TRACKER_FILE = "log_date_tracker.txt"
S3_BUCKET = "careplus-data-logs"
S3_PREFIX = "support-logs/raw/"

AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("SECRET_KEY"),
    "region_name": os.getenv("REGION"),
}

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

def upload_log_file_to_s3(file_path, bucket, key):
    s3 = boto3.client("s3", **AWS_CONFIG)
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    s3.put_object(Bucket=bucket, Key=key, Body=content)
    print(f"Uploaded log file to s3://{bucket}/{key}")

def run_log_ingestion():
    last_date = read_last_date(DATE_TRACKER_FILE)
    next_date = get_next_date(last_date)
    file_name = f"support_logs_{next_date}.log"
    log_file_full_path = os.path.join(LOGS_FOLDER, file_name)
    s3_key = f"{S3_PREFIX}support_logs_{next_date}.log"
    upload_log_file_to_s3(log_file_full_path, S3_BUCKET, s3_key)
    update_last_date(DATE_TRACKER_FILE, next_date)
    print(f"Updated tracker to {next_date}")

if __name__ == "__main__":
    run_log_ingestion()
