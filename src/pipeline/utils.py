import os
import logging
import sys
import time
import functools
from typing import Callable, Any, Tuple, Optional

import boto3
from botocore.exceptions import ClientError
from pipeline.logging_config import create_logger, log_exception
import colorlog
import pipeline.config as config

def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0, 
          exceptions: Tuple[type, ...] = (Exception,)) -> Callable:
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

class ProcessMonitor:
    """
    A utility class for monitoring process performance and tracking errors.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize ProcessMonitor.
        
        :param logger: Optional logger. If not provided, creates a default logger.
        """
        self.logger = logger or create_logger(__name__)
    
    def track_processing_time(self, func: Callable) -> Callable:
        """
        Decorator to track processing time of a function.
        
        :param func: Function to monitor
        :return: Wrapped function with processing time tracking
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                
                # Log processing time
                self.logger.info(
                    f"Function {func.__name__} completed. "
                    f"Processing time: {end_time - start_time:.2f} seconds"
                )
                
                return result
            except Exception as e:
                end_time = time.time()
                self.logger.error(
                    f"Function {func.__name__} failed. "
                    f"Processing time: {end_time - start_time:.2f} seconds. "
                    f"Error: {e}"
                )
                raise
        return wrapper
    
    def send_error_alert(self, error: Exception, context: Optional[dict] = None):
        """
        Send an alert for critical errors.
        This is a placeholder - you'd replace with actual alerting mechanism
        (e.g., email, Slack, PagerDuty)
        
        :param error: Exception that occurred
        :param context: Additional context about the error
        """
        alert_message = f"Critical Error: {type(error).__name__}\n"
        alert_message += f"Message: {str(error)}\n"
        
        if context:
            alert_message += "Context:\n"
            for key, value in context.items():
                alert_message += f"  {key}: {value}\n"
        
        self.logger.critical(alert_message)
        # TODO: Implement actual alerting mechanism (e.g., email, Slack)

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
        logger.info(f"AWS_SECRET_ACCESS_KEY: {'*' * len(os.environ.get('AWS_SECRET_ACCESS_KEY', '')) or 'NOT SET'}")
        logger.info(f"AWS_DEFAULT_REGION: {os.environ.get('AWS_DEFAULT_REGION', 'NOT SET')}")
        
        # Validate AWS credentials
        access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        
        # Comprehensive credential validation
        if not access_key:
            raise ValueError("AWS_ACCESS_KEY_ID is not set in environment variables")
        
        if not secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY is not set in environment variables")
        
        # Validate key format (basic sanity check)
        if len(access_key) < 10 or len(secret_key) < 20:
            raise ValueError("AWS credentials appear to be invalid or incomplete")
        
        # Check for default/placeholder credentials
        if access_key.startswith('AKIA') and access_key.endswith('EXAMPLE'):
            raise ValueError("Detected placeholder AWS access key. Please provide a valid key.")
        
        # Create a session with explicit credentials
        try:
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            
            # Create S3 client
            s3_client = session.client('s3')
            
            # Verify S3 access by attempting a simple operation
            try:
                # List buckets to verify credentials
                s3_client.list_buckets()
                logger.info("S3 client initialized successfully. Credentials are valid.")
            except ClientError as access_error:
                # More detailed logging for access errors
                error_code = access_error.response['Error']['Code']
                error_message = access_error.response['Error']['Message']
                
                logger.error(f"S3 Access Error: {error_code}")
                logger.error(f"Detailed Error Message: {error_message}")
                
                if error_code == 'InvalidClientTokenId':
                    raise ValueError(f"Invalid AWS Access Key ID: {access_key}. Please check your credentials.") from access_error
                elif error_code == 'SignatureDoesNotMatch':
                    raise ValueError("AWS Secret Access Key is incorrect. Please verify your credentials.") from access_error
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

### AWS S3 INTERACTIONS ###
def get_s3_file_paths(bucket_name: str, prefix: str) -> dict:
    """
    Get a list of file paths from the S3 bucket and organize them into a dictionary.

    :param bucket_name: The name of the S3 bucket.
    :param prefix: The folder prefix to filter the file paths.
    :return: Dictionary of file paths organized by source folder.
    """
    try:
        s3_prefix = prefix + '/'

        paginator = s3_init().get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_prefix}
        page_iterator = paginator.paginate(**operation_parameters)
        filtered_iterator = page_iterator.search(f"Contents[?Key != '{s3_prefix}'][]")

        file_paths = {}
        for key_data in filtered_iterator:
            key = key_data['Key']
            parts = key.split('/')
            if len(parts) >= 4:  # Ensure we have [env/landing/source/filename]
                source, filename = parts[-2], parts[-1]  # Take the last two parts
                if source not in file_paths:
                    file_paths[source] = {}
                file_paths[source][filename.split('.')[0]] = f"s3://{bucket_name}/{key}"

        logger = create_logger(__name__)
        logger.info(f"Successfully retrieved file paths from S3 bucket {bucket_name}.")
        return file_paths
    except Exception as e:
        logger = create_logger(__name__)
        logger.error(f"Error retrieving file paths from S3: {e}")
        raise

@retry(max_attempts=3, delay=1.0, backoff=2.0, exceptions=(Exception,))
def download_s3_client(s3_client: boto3.client, s3_bucket_name: str, s3_folder: str, local_dir: str) -> None:
    """
    Download all files from a specified S3 folder to a local directory.

    :param s3_client: The boto3 S3 client.
    :param s3_bucket_name: The name of the S3 bucket.
    :param s3_folder: The folder within the S3 bucket to download files from.
    :param local_dir: The local directory to save downloaded files.
    """
    try:
        # Ensure the local directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # List objects in the specified S3 folder
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_folder)
        
        # Check if any objects are returned
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_key = obj['Key']
                filename = s3_key.split('/')[-1]  # Take only the last part as filename
                local_file_path = os.path.join(local_dir, filename)
                
                # Download the file
                s3_client.download_file(s3_bucket_name, s3_key, local_file_path)
                logger = create_logger(__name__)
                logger.info(f'Successfully downloaded {s3_key} to {local_file_path}')
        else:
            logger = create_logger(__name__)
            logger.warning(f'No files found in s3://{s3_bucket_name}/{s3_folder}')

    except Exception as e:
        logger = create_logger(__name__)
        logger.error(f'Error downloading files from S3: {e}', exc_info=True)
        raise