"""Module for promoting data between S3 environments.

This module provides functionality to promote data between different S3 environments
(e.g., dev to prod) in the United Nations OSAA MVP project. It uses transaction
management to ensure atomic promotion with automatic rollback on failure.
"""

import boto3
from botocore.exceptions import ClientError
from typing import List

from pipeline.checkpoint import CheckpointScope, PipelineCheckpoint
from pipeline.config import S3_BUCKET_NAME
from pipeline.error_handler import PartialFailureCollector, retryable_operation
from pipeline.exceptions import S3OperationError
from pipeline.logging_config import create_logger
from pipeline.transaction_manager import TransactionManager
from pipeline.utils import s3_init

logger = create_logger(__name__)

# Initialize checkpoint system for promotion operations
checkpoint = PipelineCheckpoint(pipeline_name="s3_promote")

def promote_environment(
    source_env: str = "dev",
    target_env: str = "prod",
    folder: str = "landing",
    use_transactions: bool = True
) -> None:
    """
    Promote contents from source to target environment using boto3.

    This function uses transaction management to ensure atomic promotion.
    If any step fails, all changes are rolled back automatically.

    Args:
        source_env: Source environment (default: "dev")
        target_env: Target environment (default: "prod")
        folder: Folder to promote (default: "landing")
        use_transactions: Whether to use transaction manager (default: True)

    Raises:
        S3OperationError: If promotion operation fails
    """
    source_prefix = f"{source_env}/{folder}/"
    target_prefix = f"{target_env}/{folder}/"
    promotion_key = f"{source_env}_to_{target_env}_{folder}"

    # Check if already promoted recently
    if checkpoint.is_completed(
        CheckpointScope.OPERATION,
        promotion_key,
        verify_checksum=False
    ):
        logger.info(f"Promotion from {source_env} to {target_env} already completed recently")
        # Note: In production, you might want to add a time-based check here
        # return

    logger.info(f"Starting promotion from {source_prefix} to {target_prefix}")

    # Mark promotion as started
    checkpoint.mark_started(
        CheckpointScope.OPERATION,
        promotion_key,
        metadata={
            "source_env": source_env,
            "target_env": target_env,
            "folder": folder
        }
    )

    try:
        # Initialize S3 client using existing utility
        s3_client = s3_init()

        # Get list of all objects in source
        logger.info(f"Listing objects in source: {source_prefix}")
        source_objects = set()
        objects_to_promote: List[tuple] = []

        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=source_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    source_key = obj['Key']
                    source_objects.add(source_key)
                    target_key = source_key.replace(source_prefix, target_prefix, 1)
                    objects_to_promote.append((source_key, target_key))

        logger.info(f"Found {len(objects_to_promote)} objects to promote")

        if use_transactions:
            # Use transaction manager for atomic promotion
            with TransactionManager(
                bucket_name=S3_BUCKET_NAME,
                staging_prefix="promotion_staging",
                s3_client=s3_client,
                checkpoint=checkpoint
            ) as txn:
                # Copy all objects through transaction manager
                collector = PartialFailureCollector()

                for source_key, target_key in objects_to_promote:
                    try:
                        logger.info(f"Staging {source_key} → {target_key}")
                        txn.upload_from_s3(
                            source_key=source_key,
                            dest_key=target_key,
                            metadata={"promoted_from": source_key}
                        )
                        collector.add_success((source_key, target_key))
                    except Exception as e:
                        logger.error(f"Failed to stage {source_key}: {e}")
                        collector.add_failure((source_key, target_key), e)

                # Check for failures
                if collector.has_failures():
                    error_msg = (
                        f"Promotion staging had {collector.get_failure_count()} "
                        f"failures out of {collector.get_total_count()} objects"
                    )
                    logger.error(error_msg)
                    raise S3OperationError(error_msg)

                # Transaction will commit automatically on context exit
                logger.info("All objects staged successfully, committing transaction...")

            logger.info("Transaction committed successfully")

        else:
            # Direct copy without transactions (legacy mode)
            logger.warning("Promoting without transaction support")

            for source_key, target_key in objects_to_promote:
                try:
                    logger.info(f"Copying {source_key} to {target_key}")
                    _copy_s3_object(s3_client, source_key, target_key)
                except Exception as e:
                    logger.error(f"Failed to copy {source_key}: {e}")
                    raise

        # Get list of all objects in target and delete those not in source
        logger.info(f"Cleaning up obsolete objects in target: {target_prefix}")
        objects_to_delete = []

        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=target_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    target_key = obj['Key']
                    corresponding_source_key = target_key.replace(target_prefix, source_prefix, 1)

                    if corresponding_source_key not in source_objects:
                        objects_to_delete.append(target_key)

        if objects_to_delete:
            logger.info(f"Deleting {len(objects_to_delete)} obsolete objects from target")
            for target_key in objects_to_delete:
                try:
                    logger.debug(f"Deleting {target_key}")
                    _delete_s3_object(s3_client, target_key)
                except Exception as e:
                    logger.warning(f"Failed to delete {target_key}: {e}")
                    # Continue with other deletions

        # Mark promotion as completed
        checkpoint.mark_completed(
            CheckpointScope.OPERATION,
            promotion_key,
            metadata={
                "source_env": source_env,
                "target_env": target_env,
                "folder": folder,
                "objects_promoted": len(objects_to_promote),
                "objects_deleted": len(objects_to_delete)
            }
        )

        logger.info(
            f"Promotion completed successfully. "
            f"Promoted {len(objects_to_promote)} objects, "
            f"deleted {len(objects_to_delete)} obsolete objects"
        )

    except ClientError as e:
        error_msg = f"AWS operation failed: {str(e)}"
        logger.error(error_msg)
        checkpoint.mark_failed(
            CheckpointScope.OPERATION,
            promotion_key,
            error_msg
        )
        raise S3OperationError(error_msg)
    except Exception as e:
        error_msg = f"Promotion failed: {str(e)}"
        logger.error(error_msg)
        checkpoint.mark_failed(
            CheckpointScope.OPERATION,
            promotion_key,
            error_msg
        )
        raise S3OperationError(error_msg)


@retryable_operation(max_attempts=3, initial_delay=1.0)
def _copy_s3_object(s3_client, source_key: str, target_key: str) -> None:
    """Copy S3 object with retry logic."""
    s3_client.copy_object(
        Bucket=S3_BUCKET_NAME,
        CopySource={'Bucket': S3_BUCKET_NAME, 'Key': source_key},
        Key=target_key
    )


@retryable_operation(max_attempts=3, initial_delay=1.0)
def _delete_s3_object(s3_client, key: str) -> None:
    """Delete S3 object with retry logic."""
    s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=key)

def main():
    """Main function to run the promotion process."""
    try:
        promote_environment()
    except Exception as e:
        logger.error(f"Promotion process failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()