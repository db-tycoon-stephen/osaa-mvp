"""
Custom exceptions for the pipeline module.

This module defines a hierarchy of exceptions to provide more
precise error handling and debugging across the pipeline.
"""


class PipelineBaseError(Exception):
    """
    Base exception for all pipeline-related errors.

    All custom exceptions in the pipeline should inherit from this class.
    Provides a common base for catching and handling pipeline-specific errors.
    """

    pass


class ConfigurationError(PipelineBaseError):
    """
    Raised when there are configuration-related issues.

    This exception is used when:
    - Required configuration parameters are missing
    - Configuration values are invalid
    - Environment setup is incorrect
    """

    pass


class S3OperationError(PipelineBaseError):
    """
    Raised for S3-specific operation errors.

    Covers issues such as:
    - Authentication failures
    - Connection problems
    - File upload/download errors
    - Bucket or object access issues
    """

    pass


class S3ConfigurationError(S3OperationError):
    """
    Raised for S3 configuration or connection issues.

    Covers problems with:
    - AWS credentials
    - S3 client initialization
    - Access configuration
    """

    pass


class IngestError(PipelineBaseError):
    """
    Raised during the data ingestion process.

    Covers errors specific to data ingestion, including:
    - File conversion problems
    - Source data validation failures
    - Ingestion pipeline interruptions
    """

    pass


class FileConversionError(IngestError):
    """
    Raised for errors during file conversion.

    Specific to issues encountered while converting files,
    such as:
    - Unsupported file formats
    - Conversion process failures
    - Data integrity issues during conversion
    """

    pass


class TransientError(PipelineBaseError):
    """
    Raised for transient errors that may succeed on retry.

    These are temporary errors such as:
    - Network timeouts
    - Throttling errors
    - Temporary service unavailability
    """

    pass


class CheckpointError(PipelineBaseError):
    """
    Raised for checkpoint system errors.

    Covers issues with:
    - Checkpoint database operations
    - Checkpoint state inconsistencies
    - Checkpoint validation failures
    """

    pass


class TransactionError(PipelineBaseError):
    """
    Raised for transaction management errors.

    Covers issues with:
    - Transaction staging failures
    - Commit/rollback failures
    - Transaction state violations
    """

    pass


class ValidationError(TransactionError):
    """
    Raised when transaction validation fails.

    Specific to validation failures during:
    - Pre-commit validation checks
    - Data integrity verification
    - Business rule validation
    """

    pass


class RollbackError(TransactionError):
    """
    Raised when transaction rollback fails.

    Specific to failures during:
    - Staging cleanup
    - State restoration
    - Resource deallocation
    """

    pass


class PartialFailureError(PipelineBaseError):
    """
    Raised when batch operations have partial failures.

    This exception indicates that some operations succeeded
    while others failed. Contains details about both
    successful and failed operations.
    """

    pass
