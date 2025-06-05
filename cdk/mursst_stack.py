from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_sns as sns,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    Duration,
    RemovalPolicy,
)
from constructs import Construct
import os
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

        # Create or import IAM role for Lambda based on environment variable
        lambda_role = None
        if 'LAMBDA_FUNCTION_ROLE' in os.environ:
            # Import existing role using ARN from environment variable
            lambda_role = iam.Role.from_role_arn(
                self, "ImportedMursstLambdaRole",
                role_arn=os.environ['LAMBDA_FUNCTION_ROLE']
            )
        else:
            # Create new role if environment variable is not set
            lambda_role = iam.Role(
                self, "MursstLambdaRole",
                assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaSQSQueueExecutionRole"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
                ]
            )

        # Create Lambda function using the determined role
        lambda_function = _lambda.Function(
            self, "MursstCmrNotificationProcessor",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_docker_build(
                path=os.path.abspath("."),
                file="lambda/Dockerfile",
                platform="linux/amd64",
            ),
            role=lambda_role,
            environment={
                "S3_BUCKET_NAME": bucket.bucket_name,
                "SECRET_ARN": "arn:aws:secretsmanager:us-west-2:444055461661:secret:mursst_lambda_edl_credentials-9dKy1C"  # Replace with your secret ARN
            },
            timeout=Duration.seconds(30),
            memory_size=2048
        )

        # Reference existing SQS queue
        with open(QUEUE_ARN_FILE, 'r') as f:
            queue_arn = f.read().strip()

        queue = sqs.Queue.from_queue_arn(
            self, "MursstCmrNotificationQueue",
            queue_arn=queue_arn
        )

        # Add SQS permissions to the role
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes"
                ],
                resources=[queue.queue_arn]
            )
        )

        # Add SQS trigger to Lambda
        lambda_function.add_event_source_mapping(
            "MursstSqsTrigger",
            event_source_arn=queue.queue_arn,
            batch_size=1
        )

        # Grant Lambda permissions to write to S3 bucket
        bucket.grant_write(lambda_function)

        # Grant Lambda permissions to access Secrets Manager
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "secretsmanager:GetSecretValue"
                ],
                resources=["arn:aws:secretsmanager:us-west-2:444055461661:secret:mursst_lambda_edl_credentials-9dKy1C"]  # Replace with your secret ARN
            )
        )

        # Create SNS topic for notifications
        notification_topic = sns.Topic(
            self, "MursstNotificationTopic",
            topic_name="mursst-lambda-notifications"
        )

        # Add email subscription to SNS topic
        sns.Subscription(
            self,
            id="MursstCmrProcessingEmailSubscription",
            topic=notification_topic,
            protocol=sns.SubscriptionProtocol.EMAIL,
            endpoint="aimee@developmentseed.org"  # Replace with your email
        )

        # Create CloudWatch alarm for Lambda invocations
        lambda_invocation_alarm = cloudwatch.Alarm(
            self, "MursstLambdaInvocationAlarm",
            metric=lambda_function.metric_invocations(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Alarm when Lambda function is invoked"
        )

        # Add SNS action to the alarm
        lambda_invocation_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )

        # Grant Lambda permissions to publish to SNS
        notification_topic.grant_publish(lambda_function)
