# CMR Notification Processing

This CDK project deploys a Lambda function that will update a virtual icechunk store at fixed time intervals.

## Prerequisites

- AWS CDK CLI installed
- Python 3.x
- AWS credentials configured
- Access to the target AWS account and region

## Set up temporary local AWS credentials using MFA

This is still a WIP. See [this document for some solutions](https://hackmd.io/5JZ0beEKQ3mI5GUAQuGbBA).

## Setup

1. Create and activate a Python virtual environment:

```bash
$ python3 -m venv .venv
$ source .venv/bin/activate
```

2. Install the required dependencies:

```bash
$ uv pip install -r requirements.txt
```

## Infrastructure Components

- **Lambda Function**: Processes the notifications from the SQS queue
- **IAM Roles**: Provides necessary permissions for the Lambda to access required AWS services

## Deployment

To deploy the infrastructure, first ensure your environment is configured with AWS credentials. 

Then run the following CDK commands:

1. Synthesize the CloudFormation template:
```bash
$ cdk synth
```

2. Deploy the stack to your AWS account:

If there is an existing role that should be attached to this lambda function, export the `LAMBDA_FUNCTION_ROLE` variable assigned to that role name, for example:

```bash
export LAMBDA_FUNCTION_ROLE=arn:aws:iam::XXX:role/lambda-role-name
```

and then run:

```bash
$ cdk deploy
```

## Useful Commands

- `cdk ls`          List all stacks in the app
- `cdk synth`       Generate the CloudFormation template
- `cdk deploy`      Deploy the stack to your AWS account/region
- `cdk diff`        Compare deployed stack with current state
- `cdk docs`        Open CDK documentation

## Configuration

The infrastructure can be configured through environment variables and CDK context. See the `app.py` file for available configuration options.
