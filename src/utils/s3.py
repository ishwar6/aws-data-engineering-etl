import json
import logging
import time
from datetime import datetime
from typing import List, Dict
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def get_client(region: str) -> boto3.client:
    """Create S3 client.

    Args:
        region (str): AWS region for the client.

    Returns:
        boto3.client: S3 client instance.
    """
    return boto3.client("s3", region_name=region)

def write_json_records(client, bucket: str, prefix: str, records: List[Dict]) -> str:
    """Write JSON records to S3 with date partitioning.

    Args:
        client (boto3.client): S3 client instance.
        bucket (str): Bucket to write to.
        prefix (str): Base key prefix.
        records (List[Dict]): Records to serialize.

    Returns:
        str: S3 URI of the stored object.
    """
    now = datetime.utcnow()
    key = f"{prefix}/{now.year:04d}/{now.month:02d}/{now.day:02d}/data_{now.strftime('%H%M%S%f')}.json"
    body = "\n".join(json.dumps(r) for r in records)
    for attempt in range(3):
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body.encode())
            return f"s3://{bucket}/{key}"
        except ClientError as exc:
            logger.error("Put object failed: %s", exc)
            time.sleep(2 ** attempt)
    raise RuntimeError("Unable to write records to S3")
