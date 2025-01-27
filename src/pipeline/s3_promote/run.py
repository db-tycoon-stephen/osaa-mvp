"""Module for promoting data between S3 environments.

This module provides functionality to promote data between different S3 environments
(e.g., dev to prod) in the United Nations OSAA MVP project.
"""

import boto3
from botocore.exceptions import ClientError

from pipeline.config import S3_BUCKET_NAME
from pipeline.exceptions import S3OperationError
from pipeline.logging_config import create_logger
from pipeline.utils import s3_init

logger = create_logger(__name__)

def promote_environment(
    source_env: str = "dev",
    target_env: str = "prod",
    folder: str = "landing",
) -> None:
    """
    Promote contents from source to target environment using boto3.

    Args:
        source_env: Source environment (default: "dev")
        target_env: Target environment (default: "prod")
        folder: Folder to promote (default: "landing")

    Raises:
        S3OperationError: If promotion operation fails
    """
    try:
        source_prefix = f"{source_env}/{folder}/"
        target_prefix = f"{target_env}/{folder}/"

        logger.info(f"Starting promotion from {source_prefix} to {target_prefix}")
        
        # Initialize S3 client using existing utility
        s3_client = s3_init()
        
        # Get list of all objects in source
        source_objects = set()
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=source_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    source_key = obj['Key']
                    source_objects.add(source_key)
                    target_key = source_key.replace(source_prefix, target_prefix, 1)
                    
                    # Copy object to new location
                    logger.info(f"Copying {source_key} to {target_key}")
                    s3_client.copy_object(
                        Bucket=S3_BUCKET_NAME,
                        CopySource={'Bucket': S3_BUCKET_NAME, 'Key': source_key},
                        Key=target_key
                    )

        # Get list of all objects in target and delete those not in source
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=target_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    target_key = obj['Key']
                    corresponding_source_key = target_key.replace(target_prefix, source_prefix, 1)
                    
                    if corresponding_source_key not in source_objects:
                        logger.info(f"Deleting {target_key} from target")
                        s3_client.delete_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=target_key
                        )
            
        logger.info("✅ Promotion completed successfully")

    except ClientError as e:
        error_msg = f"AWS operation failed: {str(e)}"
        logger.error(error_msg)
        raise S3OperationError(error_msg)
    except Exception as e:
        error_msg = f"Promotion failed: {str(e)}"
        logger.error(error_msg)
        raise S3OperationError(error_msg)

def main():
    """Main function to run the promotion process."""
    try:
        promote_environment()
    except Exception as e:
        logger.error(f"Promotion process failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()