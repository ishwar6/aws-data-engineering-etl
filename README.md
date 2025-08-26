
# AWS Real-Time ETL Pipeline

This repository contains a modular and production-ready ETL pipeline built on AWS services. The system ingests events from Kinesis, applies business transformations, performs enrichment, publishes results to downstream consumers, and stores data in an analytics-ready format.

## Folder Structure
```
├── glue_jobs
│   └── process_events
│       └── job.py
├── infra
│   ├── fargate
│   │   └── task_definition.json
│   ├── iam
│   │   ├── fargate_policy.json
│   │   ├── glue_policy.json
│   │   ├── lambda_policy.json
│   │   └── step_functions_policy.json
│   └── state_machines
│       └── etl_state_machine.json
├── lambdas
│   ├── ingestion
│   │   └── app.py
│   └── publisher
│       └── app.py
├── tasks
│   └── enrichment
│       └── main.py
```

## Deployment Steps
1. **Create Infrastructure**
   - Provision Kinesis streams for ingestion and downstream delivery with KMS encryption.
   - Create S3 buckets for raw, processed, and error zones with server-side encryption.
   - Define Glue Data Catalog database and table.
2. **Deploy Code**
   - Package and deploy Lambda functions using SAM or CloudFormation. Set environment variables for bucket names and stream identifiers.
   - Upload Glue script to Amazon S3 and create Glue job using the script and IAM role defined in `infra/iam/glue_policy.json`.
   - Register Fargate task definition from `infra/fargate/task_definition.json` and push container image to ECR.
3. **Create Step Functions State Machine**
   - Use `infra/state_machines/etl_state_machine.json` to create the workflow and link IAM role `infra/iam/step_functions_policy.json`.
4. **Configure Triggers**
   - Enable Kinesis trigger on the ingestion Lambda.
   - Configure S3 event notifications on the raw bucket to invoke Step Functions.
5. **Monitoring and Auditing**
   - Enable CloudWatch Logs for Lambda, Glue, ECS, and Step Functions.
   - Create CloudWatch dashboards for key metrics and set alarms on failure rates.

## IAM Policies
Sample IAM policies for each service role are provided under `infra/iam`. Adjust resource ARNs to match your environment and apply the principle of least privilege.

## Cost Optimization Tips
- Use Glue job bookmarks and enable job metrics to tune DPU allocation.
- Choose AWS Fargate Spot for non-critical enrichment tasks.
- Right-size Lambda memory to balance performance and cost.
- Partition processed data to reduce Athena scan costs.

## Security Considerations
- All S3 buckets and Kinesis streams should use SSE-KMS encryption.
- Attach IAM roles with least privilege and rotate credentials regularly.

## Analytics
Processed data is stored in Parquet format and partitioned by event type and date, enabling efficient queries via Amazon Athena or integration with QuickSight.
=======
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

 
