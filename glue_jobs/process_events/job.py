"""Glue job for processing raw events and writing enriched data."""
import sys
import uuid
from typing import Tuple

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType

from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "RAW_KEY",
    "PROCESSED_BUCKET",
    "ERROR_BUCKET",
    "CATALOG_DB",
    "CATALOG_TABLE"
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

def read_raw() -> DataFrame:
    """Reads raw JSON data from the specified S3 location."""
    path = f"s3://{args['RAW_BUCKET']}/{args['RAW_KEY']}"
    return spark.read.json(path)

def flatten(df: DataFrame) -> DataFrame:
    """Flattens one level of nested structures within the DataFrame."""
    fields = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for subfield in field.dataType.fields:
                fields.append(F.col(f"{field.name}.{subfield.name}").alias(f"{field.name}_{subfield.name}"))
        else:
            fields.append(F.col(field.name))
    return df.select(fields)

def add_metadata(df: DataFrame) -> DataFrame:
    """Adds metadata fields to the DataFrame."""
    return df.withColumn("processing_id", F.lit(str(uuid.uuid4()))).withColumn(
        "processed_at", F.current_timestamp()
    )

def partition_columns(df: DataFrame) -> DataFrame:
    """Creates partition columns based on the timestamp field."""
    return df.withColumn("year", F.year("timestamp")).withColumn("month", F.month("timestamp")).withColumn(
        "day", F.dayofmonth("timestamp")
    )

def separate_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Separates invalid records missing mandatory fields."""
    required = ["event_type", "timestamp"]
    condition = " and ".join([f"{c} is not null" for c in required])
    valid = df.filter(condition)
    invalid = df.exceptAll(valid)
    return valid, invalid

def validate_schema(df: DataFrame) -> DataFrame:
    """Aligns DataFrame columns with the Glue Data Catalog table."""
    catalog_df = glue_context.create_dynamic_frame.from_catalog(
        database=args["CATALOG_DB"],
        table_name=args["CATALOG_TABLE"],
    ).toDF().limit(0)
    for field in catalog_df.schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    return df.select(catalog_df.schema.fieldNames())

def write_output(df: DataFrame) -> str:
    """Writes the DataFrame to the processed zone in Parquet format."""
    output_path = f"s3://{args['PROCESSED_BUCKET']}"
    df.write.mode("append").partitionBy("event_type", "year", "month", "day").parquet(output_path)
    return output_path

def write_errors(df: DataFrame) -> None:
    """Writes invalid records to the error bucket."""
    error_path = f"s3://{args['ERROR_BUCKET']}"
    df.write.mode("append").json(error_path)

def main() -> None:
    """Runs the ETL process."""
    df = read_raw()
    df = flatten(df)
    df = add_metadata(df)
    df = partition_columns(df)
    valid, invalid = separate_invalid(df)
    valid = validate_schema(valid)
    write_output(valid)
    write_errors(invalid)
    job.commit()

if __name__ == "__main__":
    main()
