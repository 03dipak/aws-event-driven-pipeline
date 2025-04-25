import boto3
import json
import os

sns_client = boto3.client('sns')
TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    try:
        message = f"Glue job failed:\n\n{json.dumps(event, indent=2)}"
        response = sns_client.publish(
            TopicArn=TOPIC_ARN,
            Message=message,
            Subject="🚨 AWS Glue Job Failed"
        )
        return {
            "status": "Notification sent",
            "sns_response": response
        }
    except Exception as e:
        return {
            "status": "Failed to send notification",
            "error": str(e)
        }
