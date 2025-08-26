"""Fargate task performing advanced enrichment logic."""
import json
import logging
import os
from typing import Any, Dict, List

import boto3

s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def enrich(record: Dict[str, Any]) -> Dict[str, Any]:
    """Enriches a single record."""
    record["enriched"] = True
    return record

def main() -> None:
    """Entry point for the enrichment task."""
    source_bucket = os.environ["SOURCE_BUCKET"]
    source_key = os.environ["SOURCE_KEY"]
    target_bucket = os.environ["TARGET_BUCKET"]
    target_key = os.environ["TARGET_KEY"]
    obj = s3.get_object(Bucket=source_bucket, Key=source_key)
    data: List[Dict[str, Any]] = json.loads(obj["Body"].read())
    enriched = [enrich(r) for r in data]
    s3.put_object(Bucket=target_bucket, Key=target_key, Body=json.dumps(enriched).encode("utf-8"))
    logger.info(json.dumps({"processed": len(enriched)}))

if __name__ == "__main__":
    main()
