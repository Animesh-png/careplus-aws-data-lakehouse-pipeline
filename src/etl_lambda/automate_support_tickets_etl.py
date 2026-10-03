import urllib.parse
import pandas as pd
import awswrangler as wr

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"])
    input_path = f"s3://{bucket}/{key}"

    df = wr.s3.read_csv(path=input_path)

    df["created_at"] = pd.to_datetime(df["created_at"])
    df["resolved_at"] = pd.to_datetime(df["resolved_at"])
    df["num_interactions"] = df["num_interactions"].astype(int)

    df = df.drop(columns=["agent_feedback"])
    df = df.rename(columns={"IssUeCat": "issue_category"})
    df = df[df["num_interactions"] >= 0]

    priority_mapping = {"Lw": "Low", "Medum": "Medium", "Hgh": "High"}
    df["priority"] = df["priority"].replace(priority_mapping)

    final_columns = [
        "ticket_id", "created_at", "resolved_at", "agent",
        "priority", "num_interactions", "issue_category",
        "channel", "status"
    ]
    df = df[final_columns]

    file_name = key.split("/")[-1].replace(".csv", ".parquet")
    output_path = f"s3://careplus-data-logs/support-tickets/processed/{file_name}"

    wr.s3.to_parquet(df=df, path=output_path)

    print(f"Successfully processed and saved to {output_path}")
    return {"status": 200, "message": "ETL Complete"}
