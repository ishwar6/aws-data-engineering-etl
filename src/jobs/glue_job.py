import logging
from typing import Dict, List
from config import load_config
from utils import kinesis as kin
from utils import s3 as s3_util
from processing.transformer import enrich_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run() -> None:
    """Execute the streaming ETL job.

    Returns:
        None
    """
    config = load_config()
    kin_client = kin.get_client(config.region)
    s3_client = s3_util.get_client(config.region)
    raw_records = kin.read_records(kin_client, config.input_stream)
    transformed: List[Dict] = []
    for r in raw_records:
        transformed.append({"data": enrich_record(r["data"]), "partition_key": r["partition_key"]})
    s3_util.write_json_records(s3_client, config.bucket, "processed", [r["data"] for r in transformed])
    kin.put_records(kin_client, config.output_stream, transformed)
