import boto3
import zipfile
import os
import sys

def zip_lambda(lambda_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, dirs, files in os.walk(lambda_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, lambda_path)
                zipf.write(file_path, arcname)

def deploy_lambda(lambda_name, lambda_path, role_arn, region, runtime="python3.10", handler="lambda_function.lambda_handler"):
    zip_path = f"/tmp/{lambda_name}.zip"
    zip_lambda(lambda_path, zip_path)

    client = boto3.client('lambda', region_name=region)
    with open(zip_path, 'rb') as f:
        zipped_code = f.read()

    try:
        client.get_function(FunctionName=lambda_name)
        print(f"Updating Lambda: {lambda_name}")
        response_code = client.update_function_code(
            FunctionName=lambda_name,
            ZipFile=zipped_code,
            Publish=True
        )
        print(response_code)
    except client.exceptions.ResourceNotFoundException:
        print(f"Creating Lambda: {lambda_name}")
        response = client.create_function(
            FunctionName=lambda_name,
            Runtime=runtime,
            Role=role_arn,
            Handler=handler,
            Code={'ZipFile': zipped_code},
            Timeout=30,
            MemorySize=128,
            Publish=True
        )
        print(response)

if __name__ == "__main__":
    role_arn = sys.argv[1]
    region = sys.argv[2]

    lambda_filenames = ["glue_failed_job", "unzip_and_upload_to_bronze"]
    lambda_base_path = "src/lambdas/"

    for filename in lambda_filenames:
        lambda_name = filename.replace(".py", "")
        lambda_path = os.path.join(lambda_base_path, filename)
        deploy_lambda(lambda_name, lambda_path, role_arn, region)
