import boto3
import json
sns_client = boto3.client('sns')

# Step 1: Create the SNS Topic
topic_name = "GlueJobErrors"
response = sns_client.create_topic(Name=topic_name)
topic_arn = response["TopicArn"]
print(f"SNS Topic created: {topic_arn}")

# Step 2: Subscribe an email (or other endpoint)
subscription_response = sns_client.subscribe(
    TopicArn=topic_arn,
    Protocol="email",  # Options: 'email', 'sms', 'lambda', etc.
    Endpoint="03dip.vaidya@gmail.com"  # Replace with your email
)
print(f"Subscription ARN (pending confirmation): {subscription_response['SubscriptionArn']}")

# Step 3: (Optional) Set a topic policy to allow Glue or Lambda to publish
# You only need this if you're using a custom role that needs explicit permissions.
policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},  # Or "lambda.amazonaws.com"
        "Action": "SNS:Publish",
        "Resource": topic_arn
    }]
}

sns_client.set_topic_attributes(
    TopicArn=topic_arn,
    AttributeName="Policy",
    AttributeValue=json.dumps(policy)
)

print("SNS Topic setup complete and ready to receive Glue or Lambda notifications.")
