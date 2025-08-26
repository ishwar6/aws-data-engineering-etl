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
