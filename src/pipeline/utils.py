import functools
import os
import re
import time
from typing import Any, Callable, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from pipeline.logging_config import create_logger, log_exception


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[type, ...] = (Exception,),
) -> Callable:
    """
    Retry decorator with exponential backoff.

    :param max_attempts: Maximum number of retry attempts
    :param delay: Initial delay between retries
    :param backoff: Multiplier for delay between retries
    :param exceptions: Tuple of exceptions to catch and retry
    :return: Decorated function
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = create_logger(func.__module__)
            current_delay = delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(f"Attempt {attempt} failed: {e}")

                    if attempt == max_attempts:
                        logger.error(f"All {max_attempts} attempts failed")
                        raise

                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper

    return decorator


def log_aws_initialization_error(error):
    """
    Comprehensive logging for AWS S3 initialization errors.

    :param error: The exception raised during AWS S3 initialization
    """
    logger = create_logger(__name__)

    # Comprehensive error logging
    logger.critical(f"AWS S3 Initialization Failed: {error}")
    logger.critical("Troubleshooting:")
    logger.critical("1. Verify AWS credentials")
    logger.critical("2. Check IAM user permissions")
    logger.critical("3. Ensure AWS IAM user has S3 access")


def s3_init(return_session: bool = False) -> Tuple[Any, Optional[Any]]:
    """
    Initialize S3 client using STS to assume a role.

    :param return_session: If True, returns both client and session
    :return: S3 client, and optionally the session
    :raises ClientError: If S3 initialization fails
    """
    logger = create_logger(__name__)

    try:
        # Get role ARN from environment
        role_arn = os.environ.get("AWS_ROLE_ARN")
        if not role_arn:
            raise ValueError("AWS_ROLE_ARN is not set in environment variables")

        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

        # Create STS client
        sts_client = boto3.client("sts")

        # Assume role
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="OsaaMvpSession"
        )

        # Get temporary credentials
        credentials = assumed_role_object["Credentials"]

        # Create session with temporary credentials
        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=region,
        )

        # Create S3 client
        s3_client = session.client("s3")

        # Verify S3 access
        try:
            s3_client.list_buckets()
            logger.info("S3 client initialized successfully with assumed role.")
        except ClientError as access_error:
            error_code = access_error.response["Error"]["Code"]
            error_message = access_error.response["Error"]["Message"]
            logger.error(f"S3 Access Error: {error_code}")
            logger.error(f"Detailed Error Message: {error_message}")
            raise

        return (s3_client, session) if return_session else s3_client

    except Exception as e:
        logger.critical(f"Failed to initialize S3 client: {e}")
        log_aws_initialization_error(e)
        raise


# File path and naming utilities
def get_filename_from_path(file_path: str) -> str:
    """Extract filename from a given file path.

    Args:
        file_path: Full path to the file

    Returns:
        Extracted filename without extension
    """
    return os.path.splitext(os.path.basename(file_path))[0]


def standardize_filename(filename: str) -> str:
    """Standardize filename by removing special characters.

    Args:
        filename: Input filename to standardize

    Returns:
        Standardized filename with only alphanumeric characters and underscores
    """
    return re.sub(r"[^a-zA-Z0-9_]", "_", filename).lower()


def collect_file_paths(directory: str, file_extension: str) -> Dict[str, str]:
    """Collect file paths for a specific file extension in a directory.

    Args:
        directory: Directory to search for files
        file_extension: File extension to filter (e.g., '.csv')

    Returns:
        Dictionary mapping standardized filenames to full file paths
    """
    file_paths: Dict[str, str] = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                full_path = os.path.join(root, file)
                filename = get_filename_from_path(file)
                std_filename = standardize_filename(filename)
                file_paths[std_filename] = full_path
    return file_paths
