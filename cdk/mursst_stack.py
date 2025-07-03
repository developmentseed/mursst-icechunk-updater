from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sns as sns,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    RemovalPolicy,
)
from constructs import Construct
import os

class MursstStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
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
                    iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
                ]
            )

        # Create Lambda function using the determined role
        lambda_function = _lambda.Function(
            self, "MursstIcechunkUpdater",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_docker_build(
                path=os.path.abspath("."),
                file="lambda/Dockerfile",
                platform="linux/amd64",
            ),
            role=lambda_role,
            environment={
                "SECRET_ARN": "arn:aws:secretsmanager:us-west-2:444055461661:secret:mursst_lambda_edl_credentials-9dKy1C"  # Replace with your secret ARN
            },
            timeout=Duration.seconds(600),
            memory_size=10240
        )

        # Create SNS topic for notifications
        notification_topic = sns.Topic(
            self, "MursstIcechunkUpdaterNotificationTopic",
            topic_name="mursst-icechunk-updater-notifications"
        )

        # Add email subscription to SNS topic
        # Note: You'll need to confirm the subscription in your email
        sns.Subscription(
            self, "MursstIcechunkUpdaterEmailSubscription",
            topic=notification_topic,
            protocol=sns.SubscriptionProtocol.EMAIL,
            endpoint="aimee@developmentseed.org"  # Replace with your email
        )

        # Create EventBridge rule to trigger Lambda daily at 6am PT (14:00 UTC)
        daily_rule = events.Rule(
            self, "MursstDailyRule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="14",  # 6am PT = 14:00 UTC (during PDT) or 13:00 UTC (during PST)
                day="*",
                month="*",
                year="*"
            ),
            description="Trigger Mursst Lambda function daily at 6am PT"
        )

        # Add Lambda as target for the EventBridge rule
        daily_rule.add_target(targets.LambdaFunction(lambda_function))

        # Create CloudWatch alarm for Lambda errors
        lambda_error_alarm = cloudwatch.Alarm(
            self, "MursstLambdaErrorAlarm",
            metric=lambda_function.metric_errors(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Alarm when Lambda function encounters errors"
        )

        # Add SNS action to the error alarm
        lambda_error_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )

        # Create CloudWatch alarm for Lambda invocations (success notifications)
        lambda_invocation_alarm = cloudwatch.Alarm(
            self, "MursstLambdaInvocationAlarm",
            metric=lambda_function.metric_invocations(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Alarm when Lambda function is invoked successfully"
        )

        # Add SNS action to the invocation alarm
        lambda_invocation_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )

        # Grant Lambda permissions to publish to SNS
        notification_topic.grant_publish(lambda_function)

        # Add EventBridge permissions to invoke Lambda
        lambda_function.add_permission(
            "AllowEventBridgeInvoke",
            principal=iam.ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=daily_rule.rule_arn
        )

        # Grant Lambda permissions to access Secrets Manager
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "secretsmanager:GetSecretValue"
                ],
                resources=["arn:aws:secretsmanager:us-west-2:444055461661:secret:mursst_lambda_edl_credentials-9dKy1C"]  # Replace with your secret ARN
            )
        )
