"""
Enhanced error handling and logging utilities for the OSAA MVP Pipeline.

This module provides comprehensive error handling capabilities including:
- Retry logic with exponential backoff
- Partial failure collection and reporting
- Circuit breaker pattern for repeated failures
- Error categorization (transient vs permanent)
- Global exception handling
"""

import functools
import sys
import time
import traceback
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from botocore.exceptions import ClientError

from pipeline.logging_config import create_logger, log_exception

# Create a logger for this module
logger = create_logger(__name__)


class ErrorCategory(str, Enum):
    """Categories of errors for handling strategy."""
    TRANSIENT = "transient"  # Temporary errors that may succeed on retry
    PERMANENT = "permanent"  # Errors that will not resolve with retry
    UNKNOWN = "unknown"  # Uncategorized errors


class CircuitBreakerState(str, Enum):
    """States for circuit breaker pattern."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Too many failures, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class PartialFailureException(Exception):
    """Exception raised when some operations succeed and some fail.

    This exception aggregates multiple failures and allows the pipeline
    to continue processing after partial failures.

    Attributes:
        failures: List of (item, exception) tuples
        successes: List of successfully processed items
        total_count: Total number of items attempted
    """

    def __init__(
        self,
        failures: List[Tuple[Any, Exception]],
        successes: List[Any],
        message: str = "Partial failure occurred"
    ):
        self.failures = failures
        self.successes = successes
        self.total_count = len(failures) + len(successes)
        self.failure_count = len(failures)
        self.success_count = len(successes)

        failure_summary = "\n".join([
            f"  - {item}: {str(exc)[:100]}"
            for item, exc in failures[:10]  # Limit to first 10
        ])

        if len(failures) > 10:
            failure_summary += f"\n  ... and {len(failures) - 10} more failures"

        super().__init__(
            f"{message}\n"
            f"Successes: {self.success_count}/{self.total_count}\n"
            f"Failures: {self.failure_count}/{self.total_count}\n"
            f"Failed items:\n{failure_summary}"
        )


class PartialFailureCollector:
    """Collects errors during batch processing for partial failure handling.

    This class allows operations to continue processing items even when
    some fail, collecting all errors for reporting at the end.

    Example:
        collector = PartialFailureCollector()
        for item in items:
            try:
                process_item(item)
                collector.add_success(item)
            except Exception as e:
                collector.add_failure(item, e)

        collector.raise_if_failures("Processing batch failed")
    """

    def __init__(self):
        self.failures: List[Tuple[Any, Exception]] = []
        self.successes: List[Any] = []

    def add_failure(self, item: Any, exception: Exception) -> None:
        """Record a failed item.

        Args:
            item: The item that failed
            exception: The exception that occurred
        """
        self.failures.append((item, exception))
        logger.warning(f"Item failed: {item} - {str(exception)[:200]}")

    def add_success(self, item: Any) -> None:
        """Record a successful item.

        Args:
            item: The item that succeeded
        """
        self.successes.append(item)

    def has_failures(self) -> bool:
        """Check if any failures were recorded."""
        return len(self.failures) > 0

    def has_successes(self) -> bool:
        """Check if any successes were recorded."""
        return len(self.successes) > 0

    def get_failure_count(self) -> int:
        """Get the number of failures."""
        return len(self.failures)

    def get_success_count(self) -> int:
        """Get the number of successes."""
        return len(self.successes)

    def get_total_count(self) -> int:
        """Get the total number of items processed."""
        return len(self.failures) + len(self.successes)

    def raise_if_failures(self, message: str = "Operation had failures") -> None:
        """Raise PartialFailureException if any failures occurred.

        Args:
            message: Error message for the exception

        Raises:
            PartialFailureException: If any failures were recorded
        """
        if self.has_failures():
            raise PartialFailureException(
                failures=self.failures,
                successes=self.successes,
                message=message
            )

    def log_summary(self) -> None:
        """Log a summary of successes and failures."""
        total = self.get_total_count()
        if total == 0:
            logger.info("No items processed")
            return

        success_rate = (self.get_success_count() / total) * 100
        logger.info(
            f"Batch processing summary: {self.get_success_count()}/{total} "
            f"succeeded ({success_rate:.1f}%)"
        )

        if self.has_failures():
            logger.warning(f"Failed items: {self.get_failure_count()}/{total}")
            for item, exc in self.failures[:5]:  # Log first 5 failures
                logger.warning(f"  - {item}: {str(exc)[:100]}")


class CircuitBreaker:
    """Implements circuit breaker pattern to prevent cascading failures.

    The circuit breaker monitors failures and opens (stops executing)
    when a threshold is reached, giving the system time to recover.

    Attributes:
        failure_threshold: Number of failures before opening circuit
        timeout_seconds: Time to wait before attempting recovery
        state: Current circuit breaker state
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_seconds: int = 60
    ):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitBreakerState.CLOSED

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Function positional arguments
            **kwargs: Function keyword arguments

        Returns:
            Function return value

        Raises:
            Exception: If circuit is open or function fails
        """
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
                logger.info("Circuit breaker entering half-open state")
            else:
                raise Exception(
                    f"Circuit breaker is OPEN. "
                    f"Too many failures ({self.failure_count}). "
                    f"Will retry after timeout."
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True

        elapsed = datetime.now() - self.last_failure_time
        return elapsed > timedelta(seconds=self.timeout_seconds)

    def _on_success(self) -> None:
        """Handle successful execution."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.reset()
            logger.info("Circuit breaker reset to CLOSED state")

    def _on_failure(self) -> None:
        """Handle failed execution."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            logger.error(
                f"Circuit breaker OPENED after {self.failure_count} failures"
            )

    def reset(self) -> None:
        """Reset circuit breaker to closed state."""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED


def categorize_error(exception: Exception) -> ErrorCategory:
    """Categorize an error as transient or permanent.

    Args:
        exception: The exception to categorize

    Returns:
        ErrorCategory indicating if error is transient or permanent
    """
    # Network/connection errors - usually transient
    transient_error_types = (
        ConnectionError,
        TimeoutError,
        OSError,
    )

    if isinstance(exception, transient_error_types):
        return ErrorCategory.TRANSIENT

    # AWS specific errors
    if isinstance(exception, ClientError):
        error_code = exception.response.get('Error', {}).get('Code', '')

        # Transient AWS errors
        transient_codes = {
            'RequestTimeout',
            'ThrottlingException',
            'TooManyRequestsException',
            'ServiceUnavailable',
            'InternalError',
            'SlowDown',
            'RequestLimitExceeded'
        }

        if error_code in transient_codes:
            return ErrorCategory.TRANSIENT

        # Permanent AWS errors
        permanent_codes = {
            'AccessDenied',
            'InvalidAccessKeyId',
            'SignatureDoesNotMatch',
            'NoSuchBucket',
            'NoSuchKey',
            'InvalidParameter'
        }

        if error_code in permanent_codes:
            return ErrorCategory.PERMANENT

    # File errors - usually permanent
    if isinstance(exception, (FileNotFoundError, PermissionError)):
        return ErrorCategory.PERMANENT

    # Default to unknown
    return ErrorCategory.UNKNOWN


def retryable_operation(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    circuit_breaker: Optional[CircuitBreaker] = None
) -> Callable:
    """Decorator for retrying operations with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        retry_on: Tuple of exception types to retry on (None = all)
        circuit_breaker: Optional circuit breaker instance

    Returns:
        Decorated function with retry logic

    Example:
        @retryable_operation(max_attempts=5, initial_delay=2.0)
        def upload_file(file_path):
            # Upload logic
            pass
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            delay = initial_delay

            while attempt < max_attempts:
                attempt += 1

                try:
                    # Use circuit breaker if provided
                    if circuit_breaker:
                        return circuit_breaker.call(func, *args, **kwargs)
                    else:
                        return func(*args, **kwargs)

                except Exception as e:
                    # Check if we should retry this exception
                    if retry_on and not isinstance(e, retry_on):
                        logger.error(
                            f"Non-retryable exception in {func.__name__}: {e}"
                        )
                        raise

                    # Categorize error
                    error_category = categorize_error(e)

                    # Don't retry permanent errors
                    if error_category == ErrorCategory.PERMANENT:
                        logger.error(
                            f"Permanent error in {func.__name__}, "
                            f"not retrying: {e}"
                        )
                        raise

                    # Check if this is the last attempt
                    if attempt >= max_attempts:
                        logger.error(
                            f"All {max_attempts} attempts failed for "
                            f"{func.__name__}: {e}"
                        )
                        raise

                    # Calculate delay with exponential backoff
                    current_delay = min(
                        delay * (exponential_base ** (attempt - 1)),
                        max_delay
                    )

                    # Add jitter if enabled
                    if jitter:
                        import random
                        current_delay *= (0.5 + random.random() * 0.5)

                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for "
                        f"{func.__name__}: {e}. "
                        f"Retrying in {current_delay:.2f}s... "
                        f"(Error category: {error_category.value})"
                    )

                    time.sleep(current_delay)

        return wrapper

    return decorator


class ErrorHandler:
    """Centralized error handler for pipeline operations.

    This class provides a unified interface for error handling,
    including retry logic, partial failure collection, and
    circuit breaker integration.

    Example:
        error_handler = ErrorHandler()

        # Handle single operation with retry
        result = error_handler.execute_with_retry(
            lambda: upload_file(path)
        )

        # Handle batch operations with partial failure
        collector = error_handler.process_batch(
            items=files,
            process_func=process_file
        )
    """

    def __init__(
        self,
        max_retry_attempts: int = 3,
        initial_retry_delay: float = 1.0,
        enable_circuit_breaker: bool = False,
        circuit_breaker_threshold: int = 5
    ):
        self.max_retry_attempts = max_retry_attempts
        self.initial_retry_delay = initial_retry_delay
        self.circuit_breaker: Optional[CircuitBreaker] = None

        if enable_circuit_breaker:
            self.circuit_breaker = CircuitBreaker(
                failure_threshold=circuit_breaker_threshold
            )

    def execute_with_retry(
        self,
        operation: Callable,
        operation_name: str = "operation",
        **retry_kwargs
    ) -> Any:
        """Execute an operation with retry logic.

        Args:
            operation: Callable to execute
            operation_name: Name for logging
            **retry_kwargs: Additional kwargs for retryable_operation

        Returns:
            Operation return value
        """
        # Merge default retry settings with provided kwargs
        retry_config = {
            'max_attempts': self.max_retry_attempts,
            'initial_delay': self.initial_retry_delay,
            'circuit_breaker': self.circuit_breaker
        }
        retry_config.update(retry_kwargs)

        # Wrap operation with retry logic
        retryable_op = retryable_operation(**retry_config)(operation)

        return retryable_op()

    def process_batch(
        self,
        items: List[Any],
        process_func: Callable,
        continue_on_error: bool = True,
        log_progress: bool = True
    ) -> PartialFailureCollector:
        """Process a batch of items with partial failure handling.

        Args:
            items: List of items to process
            process_func: Function to apply to each item
            continue_on_error: Whether to continue after errors
            log_progress: Whether to log progress

        Returns:
            PartialFailureCollector with results
        """
        collector = PartialFailureCollector()

        for i, item in enumerate(items):
            if log_progress and (i + 1) % 10 == 0:
                logger.info(f"Processing item {i + 1}/{len(items)}")

            try:
                process_func(item)
                collector.add_success(item)
            except Exception as e:
                collector.add_failure(item, e)

                if not continue_on_error:
                    logger.error("Stopping batch processing due to error")
                    break

        collector.log_summary()
        return collector


def global_exception_handler(exc_type, exc_value, exc_traceback):
    """
    Global exception handler to log unhandled exceptions.

    :param exc_type: Exception type
    :param exc_value: Exception value
    :param exc_traceback: Exception traceback
    """
    # Log the full traceback
    logger.critical(" UNHANDLED EXCEPTION ")
    logger.critical(
        "An unexpected error occurred that was not caught by local exception handlers."
    )

    # Format traceback
    traceback_details = traceback.extract_tb(exc_traceback)
    last_frame = traceback_details[-1]

    logger.critical(f"Error Location: {last_frame.filename}:{last_frame.lineno}")
    logger.critical(f"Function: {last_frame.name}")

    # Detailed error information
    logger.critical(f"Error Type: {exc_type.__name__}")
    logger.critical(f"Error Message: {exc_value}")

    # Categorize error
    error_category = categorize_error(exc_value) if isinstance(exc_value, Exception) else ErrorCategory.UNKNOWN
    logger.critical(f"Error Category: {error_category.value}")

    # Troubleshooting guidance
    logger.critical("Troubleshooting Recommendations:")
    logger.critical("  1. Review recent code changes")
    logger.critical("  2. Check input data and configuration")
    logger.critical("  3. Verify system dependencies")
    logger.critical("  4. Consult project documentation")

    if error_category == ErrorCategory.TRANSIENT:
        logger.critical("  5. This appears to be a transient error - retry may succeed")

    # Ensure the error is still raised after logging
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


# Set the global exception handler
sys.excepthook = global_exception_handler
