CREATE EXTERNAL TABLE IF NOT EXISTS careplus_logs_parquet (
    timestamp STRING,
    log_level STRING,
    component STRING,
    ticket_id STRING,
    session_id STRING,
    ip STRING,
    response_time INT,
    cpu DOUBLE,
    event_type STRING
)
STORED AS PARQUET
LOCATION 's3://careplus-data-logs/support-logs/processed/';

SELECT *
FROM careplus_logs_parquet
LIMIT 10;
