from dataclasses import dataclass
import os

@dataclass
class Config:
    region: str
    input_stream: str
    output_stream: str
    bucket: str

def load_config() -> Config:
    """Load configuration from environment variables.

    Returns:
        Config: Loaded configuration containing region, input stream, output stream, and bucket.
    """
    return Config(
        region=os.environ.get("AWS_REGION", "us-east-1"),
        input_stream=os.environ["INPUT_STREAM"],
        output_stream=os.environ["OUTPUT_STREAM"],
        bucket=os.environ["S3_BUCKET"]
    )
