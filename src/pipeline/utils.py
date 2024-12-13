import functools
import os
import time
from typing import Any, Callable, Optional, Tuple

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
    Initialize S3 client with robust error handling and optional session return.

    :param return_session: If True, returns both client and session
    :return: S3 client, and optionally the session
    :raises ClientError: If S3 initialization fails
    """
    logger = create_logger(__name__)

    try:
        # Comprehensive logging of environment variables
        logger.info("Checking AWS Environment Variables:")
        logger.info(f"AWS_ACCESS_KEY_ID: {os.environ.get('AWS_ACCESS_KEY_ID', 'NOT SET')}")
        logger.info(
            f"AWS_SECRET_ACCESS_KEY: {'*' * len(os.environ.get('AWS_SECRET_ACCESS_KEY', '')) or 'NOT SET'}"
        )
        logger.info(f"AWS_DEFAULT_REGION: {os.environ.get('AWS_DEFAULT_REGION', 'NOT SET')}")

        # Validate AWS credentials
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

        # Comprehensive credential validation
        if not access_key:
            raise ValueError("AWS_ACCESS_KEY_ID is not set in environment variables")

        if not secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY is not set in environment variables")

        # Validate key format (basic sanity check)
        if len(access_key) < 10 or len(secret_key) < 20:
            raise ValueError("AWS credentials appear to be invalid or incomplete")

        # Check for default/placeholder credentials
        if access_key.startswith("AKIA") and access_key.endswith("EXAMPLE"):
            raise ValueError("Detected placeholder AWS access key. Please provide a valid key.")

        # Create a session with explicit credentials
        try:
            session = boto3.Session(
                aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region
            )

            # Create S3 client
            s3_client = session.client("s3")

            # Verify S3 access by attempting a simple operation
            try:
                # List buckets to verify credentials
                s3_client.list_buckets()
                logger.info("S3 client initialized successfully. Credentials are valid.")
            except ClientError as access_error:
                # More detailed logging for access errors
                error_code = access_error.response["Error"]["Code"]
                error_message = access_error.response["Error"]["Message"]

                logger.error(f"S3 Access Error: {error_code}")
                logger.error(f"Detailed Error Message: {error_message}")

                if error_code == "InvalidClientTokenId":
                    raise ValueError(
                        f"Invalid AWS Access Key ID: {access_key}. Please check your credentials."
                    ) from access_error
                elif error_code == "SignatureDoesNotMatch":
                    raise ValueError(
                        "AWS Secret Access Key is incorrect. Please verify your credentials."
                    ) from access_error
                else:
                    raise

            return (s3_client, session) if return_session else s3_client

        except Exception as session_error:
            logger.critical(f"Failed to create AWS session: {session_error}")
            log_aws_initialization_error(session_error)
            raise

    except (ValueError, ClientError) as e:
        # Store the error for later handling
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
