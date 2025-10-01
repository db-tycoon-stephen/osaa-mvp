"""Module for handling S3 synchronization of SQLMesh database files.

This module provides functionality to sync SQLMesh database files with S3,
including downloading existing DBs and uploading updated ones. It includes
retry logic and checkpoint tracking for reliable synchronization.
"""

import os
import sys
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from pipeline.checkpoint import CheckpointScope, PipelineCheckpoint
from pipeline.error_handler import retryable_operation
from pipeline.exceptions import S3OperationError
from pipeline.logging_config import create_logger
from pipeline.config import S3_BUCKET_NAME

logger = create_logger(__name__)

# Initialize checkpoint system for S3 sync operations
checkpoint = PipelineCheckpoint(pipeline_name="s3_sync")

@retryable_operation(max_attempts=3, initial_delay=2.0)
def _download_from_s3(s3_client, bucket_name: str, s3_key: str, db_path: str) -> None:
    """Download file from S3 with retry logic."""
    s3_client.download_file(bucket_name, s3_key, db_path)


@retryable_operation(max_attempts=3, initial_delay=2.0)
def _upload_to_s3(s3_client, db_path: str, bucket_name: str, s3_key: str) -> None:
    """Upload file to S3 with retry logic."""
    s3_client.upload_file(db_path, bucket_name, s3_key)


def sync_db_with_s3(operation: str, db_path: str, bucket_name: str, s3_key: str) -> None:
    """
    Sync SQLMesh database with S3.

    This function uses retry logic and checkpoint tracking to ensure
    reliable database synchronization with S3.

    Args:
        operation: Either "download" or "upload"
        db_path: Local path to the SQLMesh database file
        bucket_name: S3 bucket name
        s3_key: Key (path) in S3 bucket

    Raises:
        S3OperationError: If S3 operations fail
    """
    try:
        s3_client = boto3.client('s3')

        if operation == "download":
            # Check if already downloaded with matching checksum
            if os.path.exists(db_path) and checkpoint.is_completed(
                CheckpointScope.OPERATION,
                f"download_{s3_key}",
                db_path,
                verify_checksum=False  # Can't verify S3 object checksum before download
            ):
                logger.info(f"DB already downloaded from S3: {db_path}, skipping")
                return

            logger.info("Attempting to download DB from S3...")
            checkpoint.mark_started(
                CheckpointScope.OPERATION,
                f"download_{s3_key}",
                metadata={"bucket": bucket_name, "key": s3_key, "path": db_path}
            )

            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                # File exists, download it
                os.makedirs(os.path.dirname(db_path), exist_ok=True)

                _download_from_s3(s3_client, bucket_name, s3_key, db_path)

                checkpoint.mark_completed(
                    CheckpointScope.OPERATION,
                    f"download_{s3_key}",
                    db_path,
                    metadata={"bucket": bucket_name, "key": s3_key}
                )
                logger.info("Successfully downloaded existing DB from S3")

            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.info("No existing DB found in S3, skipping download...")
                    checkpoint.mark_completed(
                        CheckpointScope.OPERATION,
                        f"download_{s3_key}",
                        metadata={"status": "not_found"}
                    )
                else:
                    error_msg = f"Error checking S3 object: {str(e)}"
                    checkpoint.mark_failed(
                        CheckpointScope.OPERATION,
                        f"download_{s3_key}",
                        error_msg
                    )
                    raise S3OperationError(error_msg)

        elif operation == "upload":
            # Only allow uploads in prod/qa environments
            target = os.getenv('TARGET', '').lower()
            if target not in ['prod', 'qa']:
                logger.warning(f"Upload operation restricted to prod/qa targets only (current: {target})")
                return

            if not os.path.exists(db_path):
                logger.warning(f"Local DB file not found at {db_path}, skipping upload")
                return

            # Check if already uploaded with matching checksum
            if checkpoint.is_completed(
                CheckpointScope.OPERATION,
                f"upload_{s3_key}",
                db_path,
                verify_checksum=True
            ):
                logger.info(f"DB already uploaded to S3 with matching checksum, skipping")
                return

            logger.info("Uploading DB to S3...")
            checkpoint.mark_started(
                CheckpointScope.OPERATION,
                f"upload_{s3_key}",
                db_path,
                metadata={"bucket": bucket_name, "key": s3_key}
            )

            _upload_to_s3(s3_client, db_path, bucket_name, s3_key)

            checkpoint.mark_completed(
                CheckpointScope.OPERATION,
                f"upload_{s3_key}",
                db_path,
                metadata={"bucket": bucket_name, "key": s3_key}
            )
            logger.info("Successfully uploaded DB to S3")

    except Exception as e:
        error_msg = f"S3 {operation} operation failed: {str(e)}"
        logger.error(error_msg)

        # Mark checkpoint as failed
        if operation in ["download", "upload"]:
            checkpoint.mark_failed(
                CheckpointScope.OPERATION,
                f"{operation}_{s3_key}",
                error_msg,
                db_path if os.path.exists(db_path) else None
            )

        raise S3OperationError(error_msg)

def get_db_paths(db_filename: Optional[str] = "unosaa_data_pipeline.db") -> tuple[str, str]:
    """
    Get the local and S3 paths for the database file.
    
    Args:
        db_filename: Name of the database file
        
    Returns:
        Tuple of (local_path, s3_key)
    """
    local_path = f"sqlMesh/{db_filename}"
    s3_key = db_filename
    return local_path, s3_key

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ["download", "upload"]:
        print("Usage: python -m pipeline.s3_sync.run [download|upload]")
        sys.exit(1)

    operation = sys.argv[1]
    local_path, s3_key = get_db_paths()
    sync_db_with_s3(operation, local_path, S3_BUCKET_NAME, s3_key) 