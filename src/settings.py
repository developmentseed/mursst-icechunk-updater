from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional, Dict


class RuntimeSettings(BaseSettings):
    store_name: str
    icechunk_direct_prefix: str
    edl_secret_arn: Optional[str] = ""
    run_tests: bool = Field(
        default=True, description="Run expensive data equality tests"
    )
    dry_run: bool = Field(
        default=False, description="Do not commit new references to main branch"
    )
    limit_granules: Optional[int] = Field(
        default=None,
        description="Limit the number of granues added even if more are available",
    )
    local_test: bool = Field(
        default=False,
        description="Do not require access to EDL secrets, requires local EDL creds as env variables",
    )


class DeploymentSettings(RuntimeSettings):
    """Settings for CDK deployment - inherits ALL runtime settings + deployment-specific ones"""

    # Add deployment-only infrastructure settings
    stack_name: str
    stage: str
    aws_region: str = Field(default="us-west-2", description="AWS region")
    lambda_memory_size: int = Field(default=1024, description="Lambda memory in MB")
    lambda_timeout_seconds: int = Field(default=600, description="Lambda timeout in s")

    # Email for notifications
    notification_email: str = Field(
        default="contact@juliusbusecke.com", description="Email for notifications"
    )

    def create_lambda_environment(self) -> Dict[str, str]:
        """
        Convert deployment settings to Lambda environment variables.
        Only includes fields that exist in RuntimeSettings.
        """
        # Get the field names from RuntimeSettings
        runtime_field_names = set(RuntimeSettings.__fields__.keys())

        # Convert to Lambda environment format
        lambda_env = {}
        for field_name in runtime_field_names:
            value = getattr(self, field_name)
            env_var_name = field_name.upper()
            lambda_env[env_var_name] = str(value) if value is not None else ""

        return lambda_env
