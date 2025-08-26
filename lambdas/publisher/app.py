"""Lambda function for publishing records to a downstream Kinesis stream."""
import json
import logging
import os
from typing import Any, Dict, List

import boto3

kinesis = boto3.client("kinesis")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

STREAM_NAME = os.environ["DOWNSTREAM_STREAM"]

def publish(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Publishes a list of records to the Kinesis stream."""
    entries = [
        {"Data": json.dumps(r).encode("utf-8"), "PartitionKey": r.get("event_type", "default")}
        for r in records
    ]
    return kinesis.put_records(StreamName=STREAM_NAME, Records=entries)

def lambda_handler(event, context):
    """Lambda handler for publishing records."""
    records = event.get("records", [])
    response = publish(records)
    logger.info(json.dumps({"failed_record_count": response.get("FailedRecordCount", 0)}))
    return response
