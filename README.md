# AWS Glue Streaming ETL + MSK Serverless (IAM Auth)

This repository serves as a companion documentation for the AWS Big Data blog post:

**Securely process near-real-time data from Amazon MSK Serverless using an AWS Glue streaming ETL job with IAM authentication** (see the [blog post here](https://aws.amazon.com/blogs/big-data/securely-process-near-real-time-data-from-amazon-msk-serverless-using-an-aws-glue-streaming-etl-job-with-iam-authentication/))

## Overview

Streaming data is essential for real-time analytics, powering quick insights from sources like social media, sensors, logs, and clickstreams. This solution integrates **Amazon MSK Serverless** with **AWS Glue Streaming ETL** using **IAM authentication**, enabling secure, scalable, and serverless near-real-time processing.

Key benefits include:
- Fully managed, scale-on-demand Kafka through MSK Serverless
- Secure authentication and authorization via IAM
- Serverless Spark Structured Streaming via AWS Glue
- Query results with Amazon Athena
- Resilience with Glue checkpointing

## Architecture

The architecture comprises:

![Architecture](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2023/09/05/BDB_3411_architecture_image001.png)
1. **MSK Serverless cluster with IAM-enabled authentication**, receiving input from a Kafka producer on an EC2 instance.  
2. **AWS Glue streaming job** reads from MSK Serverless, writes to Amazon S3, and catalogs data in AWS Glue Data Catalog.  
3. **Amazon Athena** enables SQL-like queries on the processed data.  
4. Resources deployed and managed via AWS CloudFormation.  
5. Fully integrated throughout—serverless, secure, resilient.

**Workflow Steps**:
1. Deploy ingestion stack (VPC, MSK Serverless, EC2 producer, S3 bucket) via `vpc-mskserverless-client.yaml`.
2. Deploy processing stack (Glue database, table, connection, streaming job) via `gluejob-setup.yaml`.
3. Produce sample data into Kafka with EC2-based producer.
4. Run the Glue streaming ETL job; verify output in S3.
5. Query processed data in Athena.
6. Clean up by deleting CloudFormation stacks.
