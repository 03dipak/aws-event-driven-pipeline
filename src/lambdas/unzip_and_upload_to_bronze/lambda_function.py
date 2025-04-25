import boto3
import zipfile
import os
import tempfile
import datetime
import uuid
import logging
import json

# Initialize AWS resources
s3_resource = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
metadata_table = dynamodb.Table('bronze_metadata_table')  # Replace with your table name
stepfunctions = boto3.client('stepfunctions')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    processed_files = []

    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            zip_key = record['s3']['object']['key']

            if not zip_key.endswith(".zip"):
                logger.warning(f"Skipping non-ZIP file: {zip_key}")
                continue

            # Temporary directory to process the zip file contents
            with tempfile.TemporaryDirectory() as tmpdir:
                local_zip_path = os.path.join(tmpdir, os.path.basename(zip_key))
                s3_resource.Bucket(bucket).download_file(zip_key, local_zip_path)

                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdir)

                now = datetime.datetime.now(datetime.timezone.utc)
                process_date = now.strftime("%Y/%m/%d")
                file_date = now.strftime("%Y%m%d")

                # Process each extracted CSV file
                for csv_file in os.listdir(tmpdir):
                    if not csv_file.endswith(".csv"):
                        continue

                    extracted_file_path = os.path.join(tmpdir, csv_file)
                    matched = False

                    # Check and upload based on categories
                    for category in ["sale_orders", "customers", "products", "suppliers", "warehouses"]:
                        if category in csv_file.lower():
                            # Create unique bronze path using UUID
                            unique_suffix = str(uuid.uuid4())
                            bronze_path = f"raw/bronze/{category.replace('_', '')}_data/{process_date}/{category}_{file_date}_{unique_suffix}.csv"

                            # Upload CSV to S3
                            s3_resource.Bucket(bucket).upload_file(extracted_file_path, bronze_path)

                            # Register file metadata in DynamoDB
                            metadata_table.put_item(
                                Item={
                                    'file_key': bronze_path,
                                    'source_zip': zip_key,
                                    'upload_timestamp': now.isoformat(),
                                    'status': 'NEW'
                                }
                            )

                            logger.info(f"Uploaded and registered: {bronze_path}")
                            processed_files.append(bronze_path)
                            matched = True
                            break

                    if not matched:
                        logger.warning(f"No matching category found for file: {csv_file}")

        except Exception as e:
            logger.error(f"Error processing file {zip_key}: {str(e)}")
    
    if processed_files:
        # Trigger the parallel Bronze jobs in Step Functions
        bronze_jobs = [
            "ingest_customers",
            "ingest_products",
            "ingest_sale_orders",
            "ingest_suppliers",
            "ingest_warehouses"
        ]
        silver_dimension_jobs =[
            "silver_dimension_customers",
            "silver_dimension_products",
            "silver_dimension_suppliers",
            "silver_dimension_warehouses"
        ]
        step_input = {
            "bronze_jobs": bronze_jobs,
            "silver_dimension_jobs": silver_dimension_jobs
        }

        try:
            response = stepfunctions.start_execution(
                stateMachineArn="arn:aws:states:ap-south-1:123456789012:stateMachine:YourStateMachineName",
                input=json.dumps(step_input)
            )
            logger.info(f"Step Function triggered successfully: {response['executionArn']}")
            return {
                'statusCode': 200,
                'body': f"Processed {len(processed_files)} file(s): {processed_files}",
                "executionArn": response["executionArn"]
            }
        except Exception as e:
            logger.error(f"Error triggering Step Function: {str(e)}")
            return {
                'statusCode': 500,
                'body': f"Error triggering Step Function: {str(e)}"
            }

    return {
        'statusCode': 200,
        'body': "No files processed."
    }