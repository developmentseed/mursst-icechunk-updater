# CMR Notification Processing

This CDK project deploys a Lambda function that processes notifications from NASA's Common Metadata Repository (CMR) via an SQS queue. The infrastructure is designed to handle subscription notifications from CMR and process them accordingly.

## Prerequisites

- AWS CDK CLI installed
- Python 3.x
- AWS credentials configured
- Access to the target AWS account and region

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

- **SQS Queue**: Receives notifications from CMR subscription
- **Lambda Function**: Processes the notifications from the SQS queue
- **IAM Roles**: Provides necessary permissions for the Lambda to access SQS and other required AWS services

## Deployment

To deploy the infrastructure:

1. Synthesize the CloudFormation template:
```bash
$ cdk synth
```

2. Deploy the stack to your AWS account:
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

## Maintenance

To add additional dependencies, add them to your `setup.py` file and rerun:
```bash
$ uv pip install -r requirements.txt
```
