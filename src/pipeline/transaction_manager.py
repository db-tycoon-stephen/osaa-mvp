"""Transaction manager for atomic S3 operations with staging and rollback.

This module provides transaction support for S3 operations, enabling atomic
uploads and promotions with automatic rollback on failure. It uses a staging
area approach where files are first uploaded to a temporary location, validated,
and then atomically moved to the production location.

Key features:
- Atomic S3 operations (staging → production)
- Automatic rollback on failure
- Support for batch operations
- Integration with checkpoint system
- Validation hooks for data verification
"""

import os
import tempfile
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from pipeline.checkpoint import CheckpointScope, PipelineCheckpoint
from pipeline.error_handler import retryable_operation
from pipeline.logging_config import create_logger

logger = create_logger(__name__)


class TransactionState(str, Enum):
    """States for transaction lifecycle."""
    CREATED = "created"
    STAGED = "staged"
    VALIDATED = "validated"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


class TransactionError(Exception):
    """Base exception for transaction errors."""
    pass


class ValidationError(TransactionError):
    """Raised when transaction validation fails."""
    pass


class RollbackError(TransactionError):
    """Raised when rollback fails."""
    pass


class S3Transaction:
    """Represents a single S3 transaction with staging and commit phases.

    This class manages a single atomic S3 operation, tracking the staging
    location, production location, and transaction state.

    Attributes:
        transaction_id: Unique identifier for this transaction
        staging_key: S3 key for staging location
        production_key: S3 key for production location
        state: Current transaction state
        metadata: Additional transaction metadata
    """

    def __init__(
        self,
        production_key: str,
        staging_prefix: str = "staging",
        metadata: Optional[Dict] = None
    ):
        self.transaction_id = str(uuid.uuid4())
        self.production_key = production_key
        self.staging_key = f"{staging_prefix}/{self.transaction_id}/{production_key}"
        self.state = TransactionState.CREATED
        self.metadata = metadata or {}
        self.created_at = datetime.now()
        self.committed_at: Optional[datetime] = None
        self.rolled_back_at: Optional[datetime] = None

        logger.debug(
            f"Created transaction {self.transaction_id} "
            f"for {production_key} → {self.staging_key}"
        )

    def mark_staged(self) -> None:
        """Mark transaction as staged."""
        self.state = TransactionState.STAGED
        logger.debug(f"Transaction {self.transaction_id} marked as staged")

    def mark_validated(self) -> None:
        """Mark transaction as validated."""
        self.state = TransactionState.VALIDATED
        logger.debug(f"Transaction {self.transaction_id} marked as validated")

    def mark_committed(self) -> None:
        """Mark transaction as committed."""
        self.state = TransactionState.COMMITTED
        self.committed_at = datetime.now()
        logger.info(f"Transaction {self.transaction_id} committed successfully")

    def mark_rolled_back(self) -> None:
        """Mark transaction as rolled back."""
        self.state = TransactionState.ROLLED_BACK
        self.rolled_back_at = datetime.now()
        logger.warning(f"Transaction {self.transaction_id} rolled back")

    def mark_failed(self) -> None:
        """Mark transaction as failed."""
        self.state = TransactionState.FAILED
        logger.error(f"Transaction {self.transaction_id} failed")


class TransactionManager:
    """Manages atomic S3 operations with staging, validation, and rollback.

    This class provides a transaction-based approach to S3 operations,
    ensuring that files are atomically promoted from staging to production
    with automatic rollback on failure.

    Example:
        with TransactionManager(bucket_name="my-bucket") as txn:
            # Upload to staging
            txn.upload_file(local_path, s3_key)

            # Validate (optional)
            txn.add_validator(lambda key: validate_file(key))

            # Commit (automatic on context exit if no errors)
        # Files are now in production location
    """

    def __init__(
        self,
        bucket_name: str,
        staging_prefix: str = "staging",
        s3_client: Optional[Any] = None,
        checkpoint: Optional[PipelineCheckpoint] = None,
        enable_checkpoints: bool = True
    ):
        """Initialize transaction manager.

        Args:
            bucket_name: S3 bucket name
            staging_prefix: Prefix for staging area
            s3_client: Optional boto3 S3 client (created if not provided)
            checkpoint: Optional checkpoint manager
            enable_checkpoints: Whether to use checkpoint system
        """
        self.bucket_name = bucket_name
        self.staging_prefix = staging_prefix
        self.s3_client = s3_client or boto3.client('s3')
        self.checkpoint = checkpoint
        self.enable_checkpoints = enable_checkpoints

        self.transactions: List[S3Transaction] = []
        self.validators: List[Callable[[str], bool]] = []
        self.committed = False
        self.rolled_back = False

        logger.info(
            f"Initialized TransactionManager for bucket {bucket_name} "
            f"with staging prefix {staging_prefix}"
        )

    def add_validator(self, validator: Callable[[str], bool]) -> None:
        """Add a validation function to run before commit.

        Args:
            validator: Function that takes an S3 key and returns True if valid

        Example:
            txn.add_validator(lambda key: check_file_size(key) > 0)
        """
        self.validators.append(validator)
        logger.debug(f"Added validator function")

    @retryable_operation(max_attempts=3, initial_delay=1.0)
    def upload_file(
        self,
        local_path: str,
        s3_key: str,
        metadata: Optional[Dict] = None
    ) -> S3Transaction:
        """Upload a file to staging area.

        Args:
            local_path: Path to local file
            s3_key: Destination S3 key (production location)
            metadata: Optional file metadata

        Returns:
            S3Transaction object

        Raises:
            TransactionError: If upload fails
        """
        if not os.path.exists(local_path):
            raise TransactionError(f"Local file not found: {local_path}")

        # Create transaction
        txn = S3Transaction(
            production_key=s3_key,
            staging_prefix=self.staging_prefix,
            metadata=metadata
        )

        try:
            # Check checkpoint if enabled
            if self.enable_checkpoints and self.checkpoint:
                if self.checkpoint.is_completed(
                    CheckpointScope.FILE,
                    s3_key,
                    local_path
                ):
                    logger.info(
                        f"File {local_path} already processed "
                        f"(checkpoint exists), skipping upload"
                    )
                    txn.mark_committed()
                    return txn

                # Mark as started
                self.checkpoint.mark_started(
                    CheckpointScope.FILE,
                    s3_key,
                    local_path,
                    metadata={"transaction_id": txn.transaction_id}
                )

            # Upload to staging
            logger.info(f"Uploading {local_path} to staging: {txn.staging_key}")

            extra_args = {}
            if metadata:
                extra_args['Metadata'] = {k: str(v) for k, v in metadata.items()}

            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                txn.staging_key,
                ExtraArgs=extra_args if extra_args else None
            )

            txn.mark_staged()
            self.transactions.append(txn)

            logger.info(f"Successfully uploaded to staging: {txn.staging_key}")
            return txn

        except Exception as e:
            txn.mark_failed()
            error_msg = f"Failed to upload {local_path} to staging: {e}"
            logger.error(error_msg)
            raise TransactionError(error_msg) from e

    @retryable_operation(max_attempts=3, initial_delay=1.0)
    def upload_from_s3(
        self,
        source_key: str,
        dest_key: str,
        metadata: Optional[Dict] = None
    ) -> S3Transaction:
        """Copy an S3 object to staging area.

        Args:
            source_key: Source S3 key
            dest_key: Destination S3 key (production location)
            metadata: Optional object metadata

        Returns:
            S3Transaction object

        Raises:
            TransactionError: If copy fails
        """
        # Create transaction
        txn = S3Transaction(
            production_key=dest_key,
            staging_prefix=self.staging_prefix,
            metadata=metadata
        )

        try:
            # Check if source exists
            self.s3_client.head_object(Bucket=self.bucket_name, Key=source_key)

            # Copy to staging
            logger.info(f"Copying {source_key} to staging: {txn.staging_key}")

            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}

            extra_args = {}
            if metadata:
                extra_args['Metadata'] = {k: str(v) for k, v in metadata.items()}
                extra_args['MetadataDirective'] = 'REPLACE'

            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=txn.staging_key,
                **extra_args
            )

            txn.mark_staged()
            self.transactions.append(txn)

            logger.info(f"Successfully copied to staging: {txn.staging_key}")
            return txn

        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                error_msg = f"Source key not found: {source_key}"
            else:
                error_msg = f"Failed to copy {source_key} to staging: {e}"

            txn.mark_failed()
            logger.error(error_msg)
            raise TransactionError(error_msg) from e

    def validate(self) -> None:
        """Run all validators on staged files.

        Raises:
            ValidationError: If any validator fails
        """
        if not self.transactions:
            logger.warning("No transactions to validate")
            return

        logger.info(f"Validating {len(self.transactions)} transactions")

        for txn in self.transactions:
            if txn.state != TransactionState.STAGED:
                logger.warning(
                    f"Skipping validation for transaction {txn.transaction_id} "
                    f"in state {txn.state}"
                )
                continue

            for i, validator in enumerate(self.validators):
                try:
                    logger.debug(
                        f"Running validator {i+1}/{len(self.validators)} "
                        f"for {txn.staging_key}"
                    )

                    if not validator(txn.staging_key):
                        raise ValidationError(
                            f"Validator {i+1} failed for {txn.staging_key}"
                        )

                except Exception as e:
                    error_msg = f"Validation failed for {txn.staging_key}: {e}"
                    logger.error(error_msg)
                    raise ValidationError(error_msg) from e

            txn.mark_validated()

        logger.info("All validations passed")

    @retryable_operation(max_attempts=3, initial_delay=1.0)
    def commit(self) -> None:
        """Commit all transactions by moving files from staging to production.

        This operation is atomic - either all files are moved or none are.

        Raises:
            TransactionError: If commit fails
        """
        if self.committed:
            logger.warning("Transaction already committed")
            return

        if not self.transactions:
            logger.info("No transactions to commit")
            self.committed = True
            return

        # Run validation if validators are registered
        if self.validators:
            self.validate()

        logger.info(f"Committing {len(self.transactions)} transactions")

        committed_transactions = []
        try:
            for txn in self.transactions:
                # Skip already committed transactions
                if txn.state == TransactionState.COMMITTED:
                    logger.debug(
                        f"Transaction {txn.transaction_id} already committed"
                    )
                    continue

                if txn.state not in [TransactionState.STAGED, TransactionState.VALIDATED]:
                    raise TransactionError(
                        f"Transaction {txn.transaction_id} in invalid state "
                        f"for commit: {txn.state}"
                    )

                # Copy from staging to production
                logger.info(
                    f"Committing {txn.staging_key} → {txn.production_key}"
                )

                self.s3_client.copy_object(
                    CopySource={'Bucket': self.bucket_name, 'Key': txn.staging_key},
                    Bucket=self.bucket_name,
                    Key=txn.production_key
                )

                # Mark as committed
                txn.mark_committed()
                committed_transactions.append(txn)

                # Update checkpoint if enabled
                if self.enable_checkpoints and self.checkpoint:
                    self.checkpoint.mark_completed(
                        CheckpointScope.FILE,
                        txn.production_key,
                        metadata={"transaction_id": txn.transaction_id}
                    )

                logger.debug(f"Transaction {txn.transaction_id} committed")

            self.committed = True
            logger.info(f"Successfully committed {len(committed_transactions)} transactions")

        except Exception as e:
            error_msg = f"Commit failed, initiating rollback: {e}"
            logger.error(error_msg)

            # Mark failed transactions
            for txn in self.transactions:
                if txn.state != TransactionState.COMMITTED:
                    txn.mark_failed()

            # Attempt rollback
            try:
                self.rollback()
            except Exception as rollback_error:
                logger.critical(f"Rollback failed: {rollback_error}")

            raise TransactionError(error_msg) from e

    def rollback(self) -> None:
        """Rollback all transactions by deleting staged files.

        This is called automatically on context exit if commit fails.

        Raises:
            RollbackError: If rollback fails
        """
        if self.rolled_back:
            logger.warning("Transaction already rolled back")
            return

        if not self.transactions:
            logger.info("No transactions to rollback")
            return

        logger.warning(f"Rolling back {len(self.transactions)} transactions")

        errors = []
        for txn in self.transactions:
            try:
                # Delete staging file
                logger.debug(f"Deleting staging file: {txn.staging_key}")

                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=txn.staging_key
                )

                txn.mark_rolled_back()

                # Mark checkpoint as failed if enabled
                if self.enable_checkpoints and self.checkpoint:
                    self.checkpoint.mark_failed(
                        CheckpointScope.FILE,
                        txn.production_key,
                        f"Transaction {txn.transaction_id} rolled back"
                    )

            except Exception as e:
                error_msg = f"Failed to delete staging file {txn.staging_key}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)

        self.rolled_back = True

        if errors:
            raise RollbackError(
                f"Rollback completed with {len(errors)} errors:\n" +
                "\n".join(errors)
            )

        logger.info("Rollback completed successfully")

    def cleanup_staging(self) -> None:
        """Clean up staging area by deleting committed transaction files.

        This should be called after successful commit to free up space.
        """
        if not self.committed:
            logger.warning("Cannot cleanup - transaction not committed")
            return

        logger.info(f"Cleaning up staging area for {len(self.transactions)} transactions")

        for txn in self.transactions:
            if txn.state == TransactionState.COMMITTED:
                try:
                    self.s3_client.delete_object(
                        Bucket=self.bucket_name,
                        Key=txn.staging_key
                    )
                    logger.debug(f"Deleted staging file: {txn.staging_key}")
                except Exception as e:
                    logger.warning(
                        f"Failed to delete staging file {txn.staging_key}: {e}"
                    )

        logger.info("Staging cleanup completed")

    def get_statistics(self) -> Dict:
        """Get transaction statistics.

        Returns:
            Dictionary with transaction statistics
        """
        stats = {
            "total": len(self.transactions),
            "committed": 0,
            "rolled_back": 0,
            "failed": 0,
            "pending": 0
        }

        for txn in self.transactions:
            if txn.state == TransactionState.COMMITTED:
                stats["committed"] += 1
            elif txn.state == TransactionState.ROLLED_BACK:
                stats["rolled_back"] += 1
            elif txn.state == TransactionState.FAILED:
                stats["failed"] += 1
            else:
                stats["pending"] += 1

        return stats

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic commit/rollback."""
        if exc_type is None:
            # No exception - commit transactions
            try:
                self.commit()
                self.cleanup_staging()
            except Exception as e:
                logger.error(f"Failed to commit transactions: {e}")
                return False
        else:
            # Exception occurred - rollback
            logger.error(f"Exception in transaction context: {exc_val}")
            try:
                self.rollback()
            except Exception as rollback_error:
                logger.critical(f"Failed to rollback: {rollback_error}")

        # Let the exception propagate
        return False
