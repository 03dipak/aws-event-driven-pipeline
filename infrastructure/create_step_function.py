import boto3
import json
import os
import sys

def create_or_update_step_function(role_arn, state_machine_name, json_file_path):
    # Check if the state machine definition file exists
    if not os.path.exists(json_file_path):
        print(f"State machine definition file not found: {json_file_path}")
        sys.exit(1)

    # Load the JSON definition
    with open(json_file_path, 'r') as file:
        state_machine_definition = json.load(file)

    # Initialize Step Functions client
    stepfunctions_client = boto3.client('stepfunctions')

    state_machine_arn = f"arn:aws:states:ap-south-1:{role_arn.split(':')[4]}:stateMachine:{state_machine_name}"

    try:
        # Try to describe the state machine
        response = stepfunctions_client.describe_state_machine(
            stateMachineArn=state_machine_arn
        )
        print("Updating state machine")

        update_response = stepfunctions_client.update_state_machine(
            stateMachineArn=response['stateMachineArn'],
            definition=json.dumps(state_machine_definition),
            roleArn=role_arn
        )

        print(f"State machine updated successfully: {update_response['stateMachineArn']}")
    
    except stepfunctions_client.exceptions.StateMachineDoesNotExist:
        print("Creating state machine")

        create_response = stepfunctions_client.create_state_machine(
            name=state_machine_name,
            roleArn=role_arn,
            definition=json.dumps(state_machine_definition),
            type='STANDARD'
        )

        print(f"State machine created successfully: {create_response['stateMachineArn']}")
        return create_response

    except Exception as e:
        print(f"Error creating or updating Step Function: {str(e)}")
        return None

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python create_or_update_step_function.py <role_arn> <state_machine_name> <json_file_path>")
        sys.exit(1)

    role_arn = sys.argv[1]
    state_machine_name = sys.argv[2]
    json_file_path = sys.argv[3]

    create_or_update_step_function(role_arn, state_machine_name, json_file_path)
