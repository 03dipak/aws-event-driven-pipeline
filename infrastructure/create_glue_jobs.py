import boto3
import sys

def create_glue_job(job_name, role_arn, script_location, bucket_name, project_lib_path, glue_version="4.0", worker_type="Standard", worker_count=2, rds_connection_name="my-rds-mysql-connection"):
    client = boto3.client('glue')

    # Define Default Arguments
    default_arguments = {
        "--enable-glue-datacatalog": "true",
        "--job-bookmark-option": "job-bookmark-enable",
        "--TempDir": f"s3://{bucket_name}/code/temp/{job_name}/",
        "--S3_TARGET_PATH": f"s3://{bucket_name}/analytics/",
        "--extra-py-files": project_lib_path,
        "--job-language": "python"
    }

    try:
        response = client.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            GlueVersion=glue_version,
            WorkerType=worker_type,
            NumberOfWorkers=worker_count,
            Timeout=2880,  # in minutes
            DefaultArguments=default_arguments,
            Connections={
                "Connections": [rds_connection_name]
            }
        )
        print(f"Job {job_name} created successfully!")
    except client.exceptions.AlreadyExistsException:
        response = client.update_job(
            JobName=job_name,
            JobUpdate={
                'Role': role_arn,
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': script_location,
                    'PythonVersion': '3'
                },
                'GlueVersion': glue_version,
                'WorkerType': worker_type,
                'NumberOfWorkers': worker_count,
                'Timeout': 2880,
                'DefaultArguments': default_arguments,
                'Connections': {
                    "Connections": [rds_connection_name]
                }   
            }
        )
        print(f"Job {job_name} updated successfully!")

    return response

if __name__ == '__main__':
    job_name = sys.argv[1]
    role_arn = sys.argv[2]
    script_location = sys.argv[3]
    bucket_name = sys.argv[4]
    project_lib_path = sys.argv[5]

    create_glue_job(job_name, role_arn, script_location, bucket_name, project_lib_path)
