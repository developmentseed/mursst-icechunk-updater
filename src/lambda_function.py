"""
AWS Lambda Handler for MURSST Icechunk Updater

This is a thin handler that orchestrates the business logic and handles
Lambda-specific concerns like event processing, error handling, and response formatting.
"""

import json
import logging
import os
from typing import Dict, Any

# Import business logic
from src.updater import MursstUpdater, get_secret_from_aws
from src.settings import RuntimeSettings
from src.exceptions import GranuleSearchError

# Configure logging for Lambda
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def setup_earthdata_credentials() -> None:
    """
    Setup Earthdata credentials from AWS Secrets Manager or environment.

    Raises:
        Exception: If credentials cannot be retrieved or set
    """
    if os.environ.get("LOCAL_TEST", "false").lower() == "true":
        logger.debug("LOCAL TEST detected. Using existing EDL environment variables")
        return

    logger.debug("Fetching secrets from AWS Secrets Manager")
    secret_arn = os.environ["EDL_SECRET_ARN"]
    secrets = get_secret_from_aws(secret_arn)

    os.environ["EARTHDATA_USERNAME"] = secrets["EARTHDATA_USERNAME"]
    os.environ["EARTHDATA_PASSWORD"] = secrets["EARTHDATA_PASSWORD"]
    logger.debug("Earthdata credentials set from AWS Secrets Manager")


def get_store_url(prefix, store_name) -> str:
    """
    Construct the icechunk store URL.

    Returns:
        str: The complete store URL

    """
    return os.path.join(prefix, store_name) + "/"


def create_success_response(message: str) -> Dict[str, Any]:
    """Create a successful Lambda response."""
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"status": "success", "message": message}),
    }


def create_error_response(error_message: str, status_code: int = 500) -> Dict[str, Any]:
    """Create an error Lambda response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"status": "error", "error": error_message}),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for MURSST icechunk updates.

    Args:
        event: Lambda event data (from EventBridge, API Gateway, etc.)
        context: Lambda runtime context

    Returns:
        Dict containing statusCode, headers, and body for Lambda response
    """
    logger.info(f"Function started with event: {json.dumps(event)}")

    try:
        # Load settings
        settings = RuntimeSettings()

        # Override settings from event payload
        dry_run = event.get("dry_run", settings.dry_run)
        run_tests = event.get("run_tests", settings.run_tests)
        limit_granules = event.get("limit_granules", settings.limit_granules)

        logger.info(
            f"Loaded settings: run_tests={run_tests}, dry_run={dry_run}, limit_granules={limit_granules}"
        )

        # Setup credentials
        setup_earthdata_credentials()

        # Get store configuration
        store_url = get_store_url(settings.icechunk_direct_prefix, settings.store_name)
        logger.info(f"Using icechunk store at {store_url}")

        # Initialize the updater and run the update
        updater = MursstUpdater(store_url)
        result_message = updater.update_icechunk_store(
            run_tests=run_tests,
            dry_run=dry_run,
            limit_granules=limit_granules,
            parallel=False,  # Disable parallel processing in Lambda environment
        )

        logger.info(f"Update completed successfully: {result_message}")
        return create_success_response(result_message)

    except GranuleSearchError as e:
        # Handle no new data available (via custom exception)
        error_msg = f"Granule search error: {str(e)}"
        logger.warning(error_msg)
        return create_error_response(error_msg, status_code=204)

    except Exception as e:
        # Handle unexpected errors
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return create_error_response(error_msg, status_code=500)


# For local testing
if __name__ == "__main__":
    """
    Local testing entry point.
    Set LOCAL_TEST=true and required environment variables.
    """
    test_event = {}
    test_context = {}

    # You'll need to set these for local testing (and activate the .env.dev as described in the README):
    # os.environ["LOCAL_TEST"] = "true"
    # os.environ["EARTHDATA_USERNAME"] = "your_username"
    # os.environ["EARTHDATA_PASSWORD"] = "your_password"

    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))
