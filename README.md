# AWS Data Engineering ETL

This project provides an AWS Glue job that reads JSON records from an AWS Kinesis Data Stream, enriches each record, writes the processed data to Amazon S3 in a date-partitioned layout, and publishes the transformed records to a new Kinesis stream.

## Folder Structure

```
src/
    config.py
    jobs/
        glue_job.py
    processing/
        transformer.py
    utils/
        kinesis.py
        s3.py
```

## Deployment

1. Package the contents of `src` and upload to an S3 location accessible by AWS Glue.
2. Create an AWS Glue job (Python 3) and set the job script to `src/jobs/glue_job.py`.
3. Supply job parameters for configuration:
   - `--INPUT_STREAM` name of the source Kinesis stream.
   - `--OUTPUT_STREAM` name of the destination Kinesis stream.
   - `--S3_BUCKET` name of the target S3 bucket.
   - `--AWS_REGION` AWS region, defaults to `us-east-1`.
4. Attach an IAM role with permissions for Kinesis read/write and S3 write operations.

## Running

When the job runs in AWS Glue it will:

1. Read records from the input stream.
2. Enrich each record with computed fields.
3. Store the results under `s3://<bucket>/processed/yyyy/mm/dd/`.
4. Publish the enriched records to the output stream for downstream consumers.

