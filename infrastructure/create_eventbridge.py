import boto3
import sys
import yaml
import json

def get_step_function_arn(region, step_function_name, role_arn):
    account_id = role_arn.split(":")[4]
    return f"arn:aws:states:{region}:{account_id}:stateMachine:{step_function_name}"

def create_eventbridge_rule(region, rule_name, s3_bucket, s3_prefix, event_role_arn, state_machine_arn):
    events_client = boto3.client("events", region_name=region)

    print(f"Creating rule: {rule_name}")
    events_client.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {"name": [s3_bucket]},
                "object": {"key": [{"prefix": s3_prefix}]}
            }
        }),
        State="ENABLED",
        Description=f"Trigger Step Function when files are uploaded to s3://{s3_bucket}/{s3_prefix}",
        RoleArn=event_role_arn
    )

    print("Adding target Step Function")
    events_client.put_targets(
        Rule=rule_name,
        Targets=[{
            "Id": "StepFunctionTarget",
            "Arn": state_machine_arn,
            "RoleArn": event_role_arn,
            "Input": json.dumps({
                "bucket": s3_bucket,
                "prefix": s3_prefix
            })
        }]
    )

def main():
    if len(sys.argv) != 8:
        print("Usage: python create_eventbridge_rule.py <step_function_name> <region> <s3_bucket> <s3_prefix> <role_arn>")
        sys.exit(1)

    step_function_name = sys.argv[2]
    region = sys.argv[3]
    rule_name = sys.argv[4]
    s3_bucket = sys.argv[5]
    s3_prefix = sys.argv[6]
    role_arn = sys.argv[7]

    state_machine_arn = get_step_function_arn(region, step_function_name, role_arn)

    create_eventbridge_rule(region, rule_name, s3_bucket, s3_prefix, role_arn, state_machine_arn)

if __name__ == "__main__":
    main()
