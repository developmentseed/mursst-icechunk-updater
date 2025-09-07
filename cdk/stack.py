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
from src.settings import DeploymentSettings


class MursstStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load ALL settings (runtime + deployment) with validation
        deploy_config = DeploymentSettings()
        # TODO: Refactor the SECRET_ARN into the environments (as secret?)
        env_dict = deploy_config.create_lambda_environment()
        env_dict["SECRET_ARN"] = (
            "arn:aws:secretsmanager:us-west-2:444055461661:secret:mursst_lambda_edl_credentials-9dKy1C"
        )

        # Create or import IAM role for Lambda based on environment variable
        if "LAMBDA_FUNCTION_ROLE" in os.environ:
            lambda_role = iam.Role.from_role_arn(
                self,
                f"MursstIcechunkUpdater-{deploy_config.stage}",
                role_arn=os.environ["LAMBDA_FUNCTION_ROLE"],
            )
        else:
            lambda_role = iam.Role(
                self,
                f"MursstLambdaRole-{deploy_config.stage}",
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
        lambda_function = _lambda.Function(
            self,
            f"MursstIcechunkUpdater-{deploy_config.stage}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_docker_build(
                path=os.path.abspath("."),
                file="src/Dockerfile",  # Create this file
                platform="linux/amd64",
            ),
            role=lambda_role,
            # Use deployment settings for infrastructure
            timeout=Duration.seconds(deploy_config.lambda_timeout_seconds),
            memory_size=deploy_config.lambda_memory_size,
            function_name=f"mursst-icechunk-updater-{deploy_config.stage}",
            # Convert deployment settings to runtime environment variables
            environment=env_dict,
        )

        # SNS topic
        notification_topic = sns.Topic(
            self,
            f"MursstIcechunkUpdaterNotificationTopic-{deploy_config.stage}",
            topic_name=f"mursst-icechunk-updater-notifications-{deploy_config.stage}",
        )

        # Email subscription
        notification_topic.add_subscription(
            sns_subs.EmailSubscription(deploy_config.notification_email)
        )

        # EventBridge rule
        daily_rule = events.Rule(
            self,
            f"MursstDailyRule-{deploy_config.stage}",
            schedule=events.Schedule.cron(
                minute="0",
                hour="14",  # 6am PT (PDT) / 13 UTC (PST)
                day="*",
                month="*",
                year="*",
            ),
            description=f"Trigger Mursst Lambda function daily at 6am PT ({deploy_config.stage})",
            rule_name=f"mursst-daily-rule-{deploy_config.stage}",
        )
        daily_rule.add_target(targets.LambdaFunction(lambda_function))

        # CloudWatch alarms
        lambda_error_alarm = cloudwatch.Alarm(
            self,
            f"MursstLambdaErrorAlarm-{deploy_config.stage}",
            metric=lambda_function.metric_errors(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description=f"Alarm when Lambda function encounters errors ({deploy_config.stage})",
            alarm_name=f"mursst-lambda-error-alarm-{deploy_config.stage}",
        )
        lambda_error_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )

        lambda_invocation_alarm = cloudwatch.Alarm(
            self,
            f"MursstLambdaInvocationAlarm-{deploy_config.stage}",
            metric=lambda_function.metric_invocations(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description=f"Alarm when Lambda function is invoked successfully ({deploy_config.stage})",
            alarm_name=f"mursst-lambda-invocation-alarm-{deploy_config.stage}",
        )
        lambda_invocation_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(notification_topic)
        )

        # Permissions
        notification_topic.grant_publish(lambda_function)

        lambda_function.add_permission(
            f"AllowEventBridgeInvoke-{deploy_config.stage}",
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
