import boto3
import zipfile
import os
import sys
import time
from pathlib import Path
from botocore.exceptions import ClientError

def zip_lambda(lambda_path, zip_path):
    try:
        # Try to create the directory if it doesn't exist
        os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    except FileExistsError:
        # If the directory exists, this exception will be caught and ignored
        pass
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, dirs, files in os.walk(lambda_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, lambda_path)
                zipf.write(file_path, arcname)

def deploy_lambda(lambda_name, lambda_path, role_arn, region, runtime="python3.10", handler="lambda_function.lambda_handler"):
    # Set environment variables conditionally
    environment_variables = {}
    
    # Add environment variable for Glue jobs (if lambda_name matches a Glue job)
    if 'glue_failed_job' == lambda_name.lower():
        sns_topic_arn = f"arn:aws:sns:{region}:{role_arn.split(':')[4]}:GlueJobErrors"
        environment_variables = {
            'SNS_TOPIC_ARN': sns_topic_arn  # Set the SNS_TOPIC_ARN for Glue jobs
        }

    # Zip the Lambda function code
    zip_path = f"/tmp/{lambda_name}.zip"
    zip_lambda(lambda_path, zip_path)

    client = boto3.client('lambda', region_name=region)
    with open(zip_path, 'rb') as f:
        zipped_code = f.read()

    try:
        # Try to get the function to check if it exists
        client.get_function(FunctionName=lambda_name)
        print(f"Updating Lambda: {lambda_name}")
        
        # Wait until the function is not in progress
        while True:
            try:
                # Try to update the function code
                response_code = client.update_function_code(
                    FunctionName=lambda_name,
                    ZipFile=zipped_code,
                    Publish=True
                )

                # Update the function configuration with environment variables
                if environment_variables:
                    client.update_function_configuration(
                        FunctionName=lambda_name,
                        Environment={
                            'Variables': environment_variables  # Apply the environment variables
                        }
                    )
                print("Lambda updated successfully!")
                break
            except client.exceptions.ResourceConflictException:
                print(f"Update in progress for Lambda: {lambda_name}. Retrying...")
                time.sleep(30)  # Retry after 30 seconds

    except client.exceptions.ResourceNotFoundException:
        # If the Lambda function doesn't exist, create it
        print(f"Creating Lambda: {lambda_name}")
        response = client.create_function(
            FunctionName=lambda_name,
            Runtime=runtime,
            Role=role_arn,
            Handler=handler,
            Code={'ZipFile': zipped_code},
            Timeout=30,
            MemorySize=128,
            Publish=True,
            Environment={
                'Variables': environment_variables  # Set environment variables during creation
            }
        )
        print(response)

if __name__ == "__main__":
    role_arn = sys.argv[1]
    region = sys.argv[2]

    lambda_filenames = ["glue_failed_job", "unzip_and_upload_to_bronze"]
    lambda_base_path = "src/lambdas"

    for lambda_name in lambda_filenames:
        lambda_path = Path(lambda_base_path) / lambda_name
        lambda_path = lambda_path.as_posix()  # Ensure using the correct path format
        deploy_lambda(lambda_name, lambda_path, role_arn, region)
