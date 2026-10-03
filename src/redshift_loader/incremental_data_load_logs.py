import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "careplus_db")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_TABLE = os.getenv("REDSHIFT_TABLE")
IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE")

def lambda_handler(event, context):
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    input_key = record["s3"]["object"]["key"]

    s3_input_path = f"s3://{bucket_name}/{input_key}"
    print(f"Triggered by: {s3_input_path}")

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )

    cur = conn.cursor()

    copy_sql = f"""
        COPY {REDSHIFT_TABLE}
        FROM '{s3_input_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        REGION 'eu-north-1';
    """

    cur.execute(copy_sql)
    conn.commit()

    print(f"Data successfully copied from {s3_input_path} to {REDSHIFT_TABLE}")

    cur.close()
    conn.close()
