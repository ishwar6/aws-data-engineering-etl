"""Lambda function for ingesting Kinesis events and storing raw data in S3."""
import base64
import json
import logging
import os
import uuid
from typing import Any, Dict

import boto3

s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

RAW_BUCKET = os.environ["RAW_BUCKET"]
ERROR_BUCKET = os.environ["ERROR_BUCKET"]

def validate_record(record: Dict[str, Any]) -> bool:
    """Validates mandatory fields within a record."""
    return "event_type" in record and "timestamp" in record

def save_to_s3(bucket: str, record: Dict[str, Any], prefix: str) -> str:
    """Writes a record to S3 and returns the object key."""
    key = f"{prefix}/{uuid.uuid4().hex}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(record).encode("utf-8"))
    return key

def lambda_handler(event, context):
    """Lambda handler for processing Kinesis events."""
    processed = 0
    failed = 0
    for item in event.get("Records", []):
        payload = base64.b64decode(item["kinesis"]["data"])
        try:
            record = json.loads(payload)
            if validate_record(record):
                save_to_s3(RAW_BUCKET, record, record.get("event_type", "unknown"))
                processed += 1
            else:
                save_to_s3(ERROR_BUCKET, {"record": record, "reason": "schema"}, "invalid")
                failed += 1
        except Exception as exc:
            save_to_s3(ERROR_BUCKET, {"payload": payload.decode("utf-8"), "reason": str(exc)}, "error")
            failed += 1
    logger.info(json.dumps({"processed": processed, "failed": failed}))
    return {"processed": processed, "failed": failed}
