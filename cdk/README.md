# MURSST Icechunk Updater

This AWS CDK project deploys a Lambda function that periodically checks for new MUR SST granules and appends them to a Zarr store managed by the `icechunk` library.

## Prerequisites

To develop and deploy this project, you will need the following tools installed and configured:

- **Git**: For cloning the repository.
- **Python 3.12+**: The runtime for both the CDK app and the Lambda function.
- **Node.js and npm**: Required for the AWS CDK.
- **AWS CLI**: For interacting with your AWS account. Ensure it's configured with your credentials (`aws configure`).
- **Docker**: The Lambda function is packaged as a Docker container, and Docker is required for local testing.
- **AWS SAM CLI**: For running the function locally in a simulated Lambda environment.


## Project Setup

1.  **Clone the Repository**
    ```bash
    git clone <repository-url>
    cd mursst-icechunk-updater/cdk
    ```

2.  **Set up CDK Virtual Environment**
    This environment is for running the `cdk` commands.
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Set up Lambda Virtual Environment**
    The Lambda function has its own dependencies. Setting up a virtual environment for it helps with code analysis and running tests.
    ```bash
    cd lambda
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    cd .. 
    ```

## Local Development and Testing

Running the function locally is crucial for rapid development and debugging. We use the **AWS SAM CLI** to invoke the function in a Docker container that simulates the AWS Lambda environment.

**Set up development environment**
```bash
uv sync --group dev
```

**Step 1: Synthesize the CloudFormation Template**

The SAM CLI needs the compiled CloudFormation template from your CDK app.
```bash
cdk synth
```
This will create the template at `cdk.out/MursstStack.template.json`.

**Step 2: Create an Event File**

The Lambda function is triggered by a scheduled Amazon EventBridge rule. To simulate this, create a file named `event.json`:
```bash
mkdir -p events
cat << EOF > events/event.json
{
  "version": "0",
  "id": "9cdd405f-9593-4234-995f-274a41656504",
  "detail-type": "Scheduled Event",
  "source": "aws.events",
  "account": "123456789012",
  "time": "2025-08-15T14:00:00Z",
  "region": "us-west-2",
  "resources": [
    "arn:aws:events:us-west-2:123456789012:rule/MursstDailyRule"
  ],
  "detail": {}
}
EOF
```

**Step 3: Create an Environment Variables File**

The function requires several environment variables. Create a file named `env.json` to hold them for local testing.

```json
{
  "MursstIcechunkUpdater": {
    "ICECHUNK_STORE_DIRECT": "s3://nasa-eodc-public/icechunk/MUR-JPL-L4-GLOB-v4.1-virtual-v2-p2",
    "DRY_RUN": "true",
    "LOCAL_TEST": "true",
    "EARTHDATA_USERNAME": "your-edl-username",
    "EARTHDATA_PASSWORD": "your-edl-password"
  }
}
```
**Important:**
- Replace the placeholder values for `EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD` with your Earthdata Login credentials.
- The `"LOCAL_TEST": "true"` variable tells the function to use the username/password from this file instead of trying to fetch them from AWS Secrets Manager.
- The `"DRY_RUN": "true"` variable makes sure we are not commiting the changes from this test to the main branch.

**Step 4: Invoke the Function Locally**

Now you can run the function:
```bash
sam local invoke MursstIcechunkUpdater \
  -t cdk.out/MursstStack.template.json \
  --event events/event.json \
  --env-vars env.json
```
This command will build the Docker image if it's the first time and then invoke your function handler. You will see all the logs, including your `DEBUG` messages, printed directly to your terminal.

### Special instructions for JupyterHUb

- docker
- 

## Deployment

To deploy the infrastructure to your AWS account, run the following commands.

1.  **Set IAM Role (Optional)**
    If you have a pre-existing IAM role for the Lambda function, export its ARN as an environment variable:
    ```bash
    export LAMBDA_FUNCTION_ROLE=arn:aws:iam::ACCOUNT_ID:role/your-lambda-role-name
    ```
    If this is not set, the CDK stack will create a new role with the necessary permissions.

2.  **Deploy the Stack**
    ```bash
    cdk deploy
    ```

## Useful CDK Commands

- `cdk ls`          - List all stacks in the app
- `cdk synth`       - Generate the CloudFormation template
- `cdk deploy`      - Deploy the stack to your AWS account/region
- `cdk diff`        - Compare deployed stack with current state
- `cdk docs`        - Open CDK documentation