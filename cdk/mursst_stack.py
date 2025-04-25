from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_s3 as s3,
    Duration,
    RemovalPolicy,
)
from constructs import Construct
QUEUE_ARN_FILE = '../queue_arn.txt'

class MursstStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for storing CMR notifications
        bucket = s3.Bucket(
            self, "MursstCmrNotificationsBucket",
            bucket_name="mursst-cmr-notifications",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Create IAM role for Lambda
        lambda_role = iam.Role(
            self, "MursstLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaSQSQueueExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        # Create Lambda function
        lambda_function = _lambda.Function(
            self, "MursstCmrNotificationProcessor",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            role=lambda_role,
            environment={
                "S3_BUCKET_NAME": bucket.bucket_name
            },
            timeout=Duration.seconds(30),
            memory_size=128
        )

        # Reference existing SQS queue
        with open(QUEUE_ARN_FILE, 'r') as f:
            queue_arn = f.read().strip()
            
        queue = sqs.Queue.from_queue_arn(
            self, "MursstCmrNotificationQueue",
            queue_arn=queue_arn
        )

        # Add SQS trigger to Lambda
        lambda_function.add_event_source_mapping(
            "MursstSqsTrigger",
            event_source_arn=queue.queue_arn,
            batch_size=10
        )

        # Grant Lambda permissions to write to S3 bucket
        bucket.grant_write(lambda_function) 