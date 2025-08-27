import os
import io
from typing import Any

import boto3
import pandas as pd


S3_BUCKET_ENV = "S3_BUCKET"
S3_KEY_ENV = "S3_KEY"
DYNAMODB_TABLE_ENV = "DYNAMODB_TABLE"
SES_SENDER_ENV = "SES_SENDER"
SES_RECIPIENT_ENV = "SES_RECIPIENT"
AWS_REGION_ENV = "AWS_REGION"


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply a series of transformations to the DataFrame."""
    # Standardize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Numeric columns: fill missing with 0
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # String columns: fill missing with empty string
    string_cols = df.select_dtypes(include="object").columns
    df[string_cols] = df[string_cols].fillna("")

    # Parse date column if present
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["day"] = df["date"].dt.day

    # Example calculation if columns exist
    if {"quantity", "price"}.issubset(df.columns):
        df["total"] = df["quantity"].astype(float) * df["price"].astype(float)

    # Add ingestion timestamp
    df["processed_at"] = pd.Timestamp.utcnow()

    # Remove duplicate rows
    df = df.drop_duplicates()

    return df


def run_etl(event: Any = None, context: Any = None) -> None:
    """Entry point for the ETL pipeline."""
    bucket = os.environ[S3_BUCKET_ENV]
    key = os.environ[S3_KEY_ENV]
    table_name = os.environ[DYNAMODB_TABLE_ENV]
    sender = os.environ[SES_SENDER_ENV]
    recipient = os.environ[SES_RECIPIENT_ENV]
    region = os.environ.get(AWS_REGION_ENV, "us-east-1")

    s3 = boto3.client("s3", region_name=region)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    df = transform_data(df)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_key = key.rsplit(".", 1)[0] + ".parquet"
    s3.put_object(Bucket=bucket, Key=parquet_key, Body=parquet_buffer.getvalue())

    ses = boto3.client("ses", region_name=region)
    ses.send_email(
        Source=sender,
        Destination={"ToAddresses": [recipient]},
        Message={
            "Subject": {"Data": f"File {key} processed"},
            "Body": {
                "Text": {
                    "Data": f"CSV file {key} processed and stored as {parquet_key}."
                }
            },
        },
    )

    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for record in df.to_dict(orient="records"):
            batch.put_item(Item=record)


if __name__ == "__main__":
    run_etl()
