from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
)
from constructs import Construct
import os


class MursstStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 👇 derive environment suffix from construct_id (e.g. "prod" / "staging")
        env_suffix = construct_id.split("-")[-1]

        # Create or import IAM role for Lambda based on environment variable
        if "LAMBDA_FUNCTION_ROLE" in os.environ:
            lambda_role = iam.Role.from_role_arn(
                self,
                f"ImportedMursstLambdaRole-{env_suffix}",
                role_arn=os.environ["LAMBDA_FUNCTION_ROLE"],
            )
        else:
            lambda_role = iam.Role(
                self,
                f"MursstLambdaRole-{env_suffix}",
                assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AWSLambdaBasicExecutionRole"
                    ),
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonS3FullAccess"
                    ),
                ],
            )

        # Lambda function
        lambda_function = _lambda.DockerImageFunction(
            self,
            f"MursstIcechunkUpdater-{env_suffix}",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=os.path.abspath("."),
                file="src/Dockerfile",
                platform="linux/amd64",
            ),
            role=lambda_role,
            environment={
                "SECRET_ARN": "arn:aws:secretsmanager:us-west-2:444055461661:secret:mursst_lambda_edl_credentials-9dKy1C",
            },
            timeout=Duration.seconds(600),
            memory_size=1024,
            function_name=f"mursst-icechunk-updater-{env_suffix}",
        )

        # SNS topic
        notification_topic = sns.Topic(
            self,
            f"MursstIcechunkUpdaterNotificationTopic-{env_suffix}",
            topic_name=f"mursst-icechunk-updater-notifications-{env_suffix}",
        )

        # Email subscription
        notification_topic.add_subscription(
            sns_subs.EmailSubscription("contact@juliusbusecke.com")
        )

        # EventBridge rule
        daily_rule = events.Rule(
            self,
            f"MursstDailyRule-{env_suffix}",
            schedule=events.Schedule.cron(
                minute="0",
                hour="14",  # 6am PT (PDT) / 13 UTC (PST)
                day="*",
                month="*",
                year="*",
            ),
            description=f"Trigger Mursst Lambda function daily at 6am PT ({env_suffix})",
            rule_name=f"mursst-daily-rule-{env_suffix}",
        )
        daily_rule.add_target(targets.LambdaFunction(lambda_function))

        # CloudWatch alarms
        lambda_error_alarm = cloudwatch.Alarm(
            self,
            f"MursstLambdaErrorAlarm-{env_suffix}",
            metric=lambda_function.metric_errors(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description=f"Alarm when Lambda function encounters errors ({env_suffix})",
            alarm_name=f"mursst-lambda-error-alarm-{env_suffix}",
        )
        lambda_error_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )

        lambda_invocation_alarm = cloudwatch.Alarm(
            self,
            f"MursstLambdaInvocationAlarm-{env_suffix}",
            metric=lambda_function.metric_invocations(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description=f"Alarm when Lambda function is invoked successfully ({env_suffix})",
            alarm_name=f"mursst-lambda-invocation-alarm-{env_suffix}",
        )
        lambda_invocation_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )

        # Permissions
        notification_topic.grant_publish(lambda_function)

        lambda_function.add_permission(
            f"AllowEventBridgeInvoke-{env_suffix}",
            principal=iam.ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=daily_rule.rule_arn,
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[
                    "arn:aws:secretsmanager:us-west-2:444055461661:secret:mursst_lambda_edl_credentials-9dKy1C"
                ],
            )
        )
