import json
import logging
import time
from typing import List, Dict
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def get_client(region: str) -> boto3.client:
    """Create Kinesis client.

    Args:
        region (str): AWS region for the client.

    Returns:
        boto3.client: Kinesis client instance.
    """
    return boto3.client("kinesis", region_name=region)

def read_records(client, stream_name: str, limit: int = 100) -> List[Dict]:
    """Read records from a Kinesis stream.

    Args:
        client (boto3.client): Kinesis client instance.
        stream_name (str): Name of the stream to read from.
        limit (int): Maximum number of records to fetch.

    Returns:
        List[Dict]: Parsed records containing data and partition key.
    """
    for attempt in range(3):
        try:
            response = client.describe_stream(StreamName=stream_name)
            shards = response["StreamDescription"]["Shards"]
            break
        except ClientError as exc:
            logger.error("Describe stream failed: %s", exc)
            time.sleep(2 ** attempt)
    else:
        raise RuntimeError("Unable to describe Kinesis stream")
    records: List[Dict] = []
    for shard in shards:
        shard_id = shard["ShardId"]
        iterator = client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        while iterator and len(records) < limit:
            out = client.get_records(ShardIterator=iterator, Limit=min(limit - len(records), 100))
            iterator = out.get("NextShardIterator")
            records.extend(out.get("Records", []))
            if not out.get("Records"):
                time.sleep(1.0)
    parsed: List[Dict] = []
    for item in records:
        data = json.loads(item["Data"].decode())
        parsed.append({"data": data, "partition_key": item["PartitionKey"]})
    return parsed

def put_records(client, stream_name: str, records: List[Dict]) -> None:
    """Write records to a Kinesis stream.

    Args:
        client (boto3.client): Kinesis client instance.
        stream_name (str): Name of the stream to write to.
        records (List[Dict]): Records to publish.

    Returns:
        None
    """
    entries = []
    for record in records:
        data_bytes = json.dumps(record["data"]).encode()
        entries.append({"Data": data_bytes, "PartitionKey": record.get("partition_key", "default")})
    for attempt in range(3):
        try:
            client.put_records(StreamName=stream_name, Records=entries)
            return
        except ClientError as exc:
            logger.error("Put records failed: %s", exc)
            time.sleep(2 ** attempt)
    raise RuntimeError("Unable to write records to Kinesis")
