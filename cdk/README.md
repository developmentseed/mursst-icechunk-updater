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

## Development
Running the notebooks on the veda hub is not as easy as https://docs.astral.sh/uv/guides/integration/jupyter/#using-jupyter-with-a-non-project-environment, but if you are running from an image that has uv installed you can do this in a cell:

```
!uv export --format=requirements.txt --no-hashes --no-annotate --no-header > temp_requirements
```
and then
```
!uv pip install -r temp_requirements
```



To run the notebooks on the veda hub use:

```
uv venv
```
