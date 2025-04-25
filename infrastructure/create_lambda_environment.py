import boto3
import sys
import re

# Check if sufficient arguments are passed
if len(sys.argv) < 3:
    print("Usage: python script.py <lambda_name> <role_arn>")
    sys.exit(1)

# Parse arguments: Lambda function name and Role ARN
lambda_name = sys.argv[1]
role_arn = sys.argv[2]

account_id = role_arn.split(":")[4]
# Extract account ID from the role ARN

sns_topic_arn = f"arn:aws:sns:ap-south-1:{account_id}:GlueJobErrors"  # Construct SNS Topic ARN

# Set up the Lambda client
lambda_client = boto3.client('lambda')

# Update Lambda function with the generated SNS_TOPIC_ARN environment variable
response = lambda_client.update_function_configuration(
    FunctionName=lambda_name,
    Environment={
        'Variables': {
            'SNS_TOPIC_ARN': sns_topic_arn  # Set the SNS Topic ARN as environment variable
        }
    }
)

# Print the response from Lambda update
print(f"Lambda function {lambda_name} updated with SNS_TOPIC_ARN: {sns_topic_arn}")
print(response)
