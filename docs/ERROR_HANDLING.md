# Error Handling and Idempotency Architecture

## Overview

The OSAA data pipeline implements comprehensive error handling and idempotency features to ensure reliable, fault-tolerant data processing. This document describes the architecture, design patterns, and usage of the error handling system.

## Key Features

1. **Idempotent File Processing**: All file operations can be safely retried without side effects
2. **Checkpoint/Restart Logic**: Track processed files and resume from failure points
3. **Partial Failure Handling**: Process all files and collect errors instead of failing fast
4. **Transaction Support**: Atomic operations for critical sections with rollback capability
5. **Retry Logic**: Exponential backoff for transient failures
6. **Circuit Breaker**: Prevent cascading failures in distributed systems

## Architecture Components

### 1. Checkpoint System (`checkpoint.py`)

The checkpoint system tracks processing state using DuckDB for persistence. It enables idempotent operations by recording file checksums and processing status.

#### Key Classes

- **`PipelineCheckpoint`**: Main checkpoint manager
- **`CheckpointStatus`**: Enum for checkpoint states (pending, in_progress, completed, failed)
- **`CheckpointScope`**: Enum for scope levels (pipeline, model, file, operation)

#### Checkpoint States

```
pending → in_progress → completed
                     ↓
                   failed
```

#### Usage Example

```python
from pipeline.checkpoint import CheckpointScope, PipelineCheckpoint

# Initialize checkpoint manager
checkpoint = PipelineCheckpoint(pipeline_name="ingest")

# Check if already processed
if checkpoint.is_completed(
    CheckpointScope.FILE,
    "s3://bucket/file.parquet",
    "local/file.csv",
    verify_checksum=True
):
    print("File already processed, skipping")
    return

# Mark as started
checkpoint.mark_started(
    CheckpointScope.FILE,
    "s3://bucket/file.parquet",
    "local/file.csv",
    metadata={"user": "pipeline"}
)

try:
    # Process file
    process_file("local/file.csv")

    # Mark as completed
    checkpoint.mark_completed(
        CheckpointScope.FILE,
        "s3://bucket/file.parquet",
        "local/file.csv"
    )
except Exception as e:
    # Mark as failed
    checkpoint.mark_failed(
        CheckpointScope.FILE,
        "s3://bucket/file.parquet",
        str(e),
        "local/file.csv"
    )
    raise
```

#### Checksum Verification

The checkpoint system automatically calculates MD5 checksums for files to detect changes:

```python
# File with matching checksum is skipped
if checkpoint.is_completed(scope, key, file_path, verify_checksum=True):
    # File hasn't changed since last processing
    return
```

#### Checkpoint Scopes

- **PIPELINE**: Entire pipeline run
- **MODEL**: Model-level operations (e.g., SQLMesh models)
- **FILE**: Individual file processing
- **OPERATION**: Specific operations (e.g., S3 sync, promotion)

### 2. Error Handler (`error_handler.py`)

Centralized error handling with retry logic, partial failure collection, and circuit breaker pattern.

#### Key Classes

- **`ErrorHandler`**: Main error handler with retry and batch processing
- **`PartialFailureCollector`**: Collects errors during batch processing
- **`CircuitBreaker`**: Prevents cascading failures
- **`ErrorCategory`**: Categorizes errors (transient, permanent, unknown)

#### Retry Logic with Exponential Backoff

```python
from pipeline.error_handler import retryable_operation

@retryable_operation(
    max_attempts=3,
    initial_delay=1.0,
    max_delay=60.0,
    exponential_base=2.0
)
def upload_file(file_path):
    # Upload logic that will be retried on transient failures
    s3_client.upload_file(file_path, bucket, key)
```

#### Error Categorization

The system automatically categorizes errors:

- **Transient**: Network timeouts, throttling, temporary service unavailability
- **Permanent**: Authentication failures, file not found, invalid parameters
- **Unknown**: Uncategorized errors (treated as transient)

Permanent errors are not retried automatically.

#### Partial Failure Handling

Process all items and collect failures instead of failing fast:

```python
from pipeline.error_handler import PartialFailureCollector

collector = PartialFailureCollector()

for file in files:
    try:
        process_file(file)
        collector.add_success(file)
    except Exception as e:
        collector.add_failure(file, e)
        continue  # Don't stop on first error

# Log summary
collector.log_summary()

# Raise exception if any failures occurred
if collector.has_failures():
    collector.raise_if_failures("File processing had partial failures")
```

#### Circuit Breaker Pattern

Prevent cascading failures by opening the circuit after repeated failures:

```python
from pipeline.error_handler import CircuitBreaker

circuit_breaker = CircuitBreaker(
    failure_threshold=5,  # Open after 5 failures
    timeout_seconds=60    # Wait 60 seconds before retry
)

# Execute with circuit breaker protection
result = circuit_breaker.call(risky_operation, arg1, arg2)
```

Circuit breaker states:
- **CLOSED**: Normal operation
- **OPEN**: Too many failures, rejecting requests
- **HALF_OPEN**: Testing if service recovered

### 3. Transaction Manager (`transaction_manager.py`)

Provides atomic S3 operations with staging, validation, and automatic rollback.

#### Transaction Flow

```
1. Stage files to temporary location
2. Optionally validate staged files
3. Atomically commit to production location
4. Clean up staging area

If any step fails → automatic rollback
```

#### Usage Example

```python
from pipeline.transaction_manager import TransactionManager

with TransactionManager(
    bucket_name="my-bucket",
    staging_prefix="staging",
    checkpoint=checkpoint
) as txn:
    # Upload to staging
    txn.upload_file(
        local_path="data/file.csv",
        s3_key="prod/data/file.parquet",
        metadata={"version": "1.0"}
    )

    # Add validation
    txn.add_validator(lambda key: validate_file_size(key) > 0)

    # Commit (automatic on context exit)
# Files are now in production, or rolled back on error
```

#### Transaction Features

- **Automatic Rollback**: If any operation fails, all staged files are deleted
- **Validation Hooks**: Add custom validation before commit
- **Checkpoint Integration**: Automatically updates checkpoints
- **Retry Support**: All operations use retry logic
- **Idempotency**: Check if already committed before re-running

### 4. Exception Hierarchy (`exceptions.py`)

Custom exception types for precise error handling:

```
PipelineBaseError (base)
├── ConfigurationError
├── S3OperationError
│   └── S3ConfigurationError
├── IngestError
│   └── FileConversionError
├── TransientError
├── CheckpointError
├── TransactionError
│   ├── ValidationError
│   └── RollbackError
└── PartialFailureError
```

## Design Patterns

### Idempotency Pattern

Ensure operations can be safely retried:

```python
# Check if already processed
if checkpoint.is_completed(file_path):
    return

# Verify file hasn't changed
current_checksum = calculate_checksum(file_path)
if checkpoint.get_checksum(file_path) == current_checksum:
    return

# Process file
process_file(file_path)

# Mark complete with checksum
checkpoint.mark_complete(file_path, current_checksum)
```

### Partial Failure Pattern

Continue processing after errors:

```python
errors = []
successes = []

for file in files:
    try:
        process_file(file)
        successes.append(file)
    except Exception as e:
        errors.append((file, e))
        continue  # Don't stop

# Report all errors at the end
if errors:
    raise PartialFailureException(errors, successes)
```

### Transaction Pattern

Atomic operations with rollback:

```python
with TransactionManager() as txn:
    # Stage data to temp location
    txn.upload_to_staging(data, staging_path)

    # Verify data
    validate_data(staging_path)

    # Atomic move to production
    txn.commit(staging_path, production_path)
# Auto-rollback on exception
```

### Retry Pattern

Exponential backoff with jitter:

```python
@retryable_operation(
    max_attempts=3,
    initial_delay=1.0,
    exponential_base=2.0,
    jitter=True
)
def unreliable_operation():
    # Will retry with delays: 1s, 2s, 4s (with jitter)
    pass
```

## Recovery Procedures

### Using the Recovery Tool

The `recover_pipeline.py` script provides manual recovery operations:

#### List Failed Operations

```bash
python scripts/recover_pipeline.py list-failed --pipeline ingest
```

#### Retry Specific File

```bash
python scripts/recover_pipeline.py retry \
    --pipeline ingest \
    --scope file \
    --key "s3://bucket/file.parquet"
```

#### Clear Checkpoints

```bash
# Clear all failed checkpoints
python scripts/recover_pipeline.py clear \
    --pipeline ingest \
    --status failed \
    --confirm

# Clear specific checkpoint
python scripts/recover_pipeline.py clear \
    --pipeline ingest \
    --scope file \
    --key "s3://bucket/file.parquet" \
    --confirm
```

#### View Statistics

```bash
python scripts/recover_pipeline.py stats --pipeline ingest
```

#### Verify Checksums

```bash
python scripts/recover_pipeline.py verify \
    --pipeline ingest \
    --limit 100
```

### Manual Recovery Steps

#### 1. Identify Failed Operations

```python
from pipeline.checkpoint import PipelineCheckpoint

checkpoint = PipelineCheckpoint("ingest")
failed = checkpoint.get_pending()

for item in failed:
    print(f"Failed: {item['file_path']}")
    print(f"Error: {item['error_message']}")
```

#### 2. Clear Checkpoint and Retry

```python
# Clear specific checkpoint
checkpoint.clear_checkpoint(
    CheckpointScope.FILE,
    "s3://bucket/file.parquet",
    "local/file.csv"
)

# Re-run pipeline
ingest.run()
```

#### 3. Force Reprocessing

```python
# Clear all checkpoints for a scope
checkpoint.clear_all_checkpoints(
    scope=CheckpointScope.FILE,
    status=CheckpointStatus.COMPLETED
)
```

## Best Practices

### 1. Always Use Checkpoints

Mark operations as started before processing and completed/failed after:

```python
checkpoint.mark_started(scope, key, file_path)
try:
    process()
    checkpoint.mark_completed(scope, key, file_path)
except Exception as e:
    checkpoint.mark_failed(scope, key, str(e), file_path)
    raise
```

### 2. Handle Partial Failures

Don't fail fast - process all items and collect errors:

```python
collector = PartialFailureCollector()
for item in items:
    try:
        process(item)
        collector.add_success(item)
    except Exception as e:
        collector.add_failure(item, e)

collector.raise_if_failures("Batch processing failed")
```

### 3. Use Transactions for Critical Operations

Wrap important S3 operations in transactions:

```python
with TransactionManager(bucket_name=bucket) as txn:
    # All operations here are atomic
    txn.upload_file(local, s3_key)
    # Auto-commit or rollback
```

### 4. Categorize Custom Errors

Help the retry logic by categorizing your errors:

```python
from pipeline.exceptions import TransientError

if is_temporary_issue:
    raise TransientError("Service temporarily unavailable")
else:
    raise PermanentError("Invalid configuration")
```

### 5. Add Validation Hooks

Validate data before committing transactions:

```python
txn.add_validator(lambda key: check_file_exists(key))
txn.add_validator(lambda key: check_file_size(key) > 1000)
```

### 6. Monitor Circuit Breakers

Log circuit breaker state changes and alert when opened:

```python
if circuit_breaker.state == CircuitBreakerState.OPEN:
    logger.critical("Circuit breaker opened - service degraded")
    send_alert()
```

### 7. Regular Checkpoint Cleanup

Periodically clean old checkpoints to prevent database bloat:

```python
# Clear completed checkpoints older than 30 days
checkpoint.clear_all_checkpoints(
    status=CheckpointStatus.COMPLETED
    # Add time-based filter in production
)
```

## Testing

### Testing Idempotency

Verify operations can be run twice with same result:

```python
def test_idempotent_processing():
    # Process once
    result1 = process_file("test.csv")

    # Process again
    result2 = process_file("test.csv")

    # Should be same result
    assert result1 == result2

    # Should only process once (check logs/checkpoints)
    assert checkpoint.is_completed(scope, key)
```

### Testing Retry Logic

Mock failures to test retry behavior:

```python
def test_retry_on_transient_error():
    mock_client = Mock()
    mock_client.upload_file.side_effect = [
        ConnectionError("Temporary failure"),
        ConnectionError("Temporary failure"),
        None  # Success on third attempt
    ]

    # Should succeed after retries
    upload_with_retry(mock_client, "file.csv")

    # Should have been called 3 times
    assert mock_client.upload_file.call_count == 3
```

### Testing Partial Failures

Verify batch processing continues after errors:

```python
def test_partial_failure_handling():
    files = ["good1.csv", "bad.csv", "good2.csv"]

    collector = process_batch(files)

    # Should have 2 successes and 1 failure
    assert collector.get_success_count() == 2
    assert collector.get_failure_count() == 1

    # Should have processed all files
    assert collector.get_total_count() == 3
```

### Testing Transaction Rollback

Verify automatic rollback on failure:

```python
def test_transaction_rollback():
    with pytest.raises(Exception):
        with TransactionManager(bucket) as txn:
            txn.upload_file("file1.csv", "key1")
            txn.upload_file("file2.csv", "key2")
            raise Exception("Simulated failure")

    # Files should not exist in production
    assert not s3_object_exists("key1")
    assert not s3_object_exists("key2")
```

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Checkpoint Statistics**
   - Number of failed checkpoints
   - Rate of checkpoint failures
   - Time to recovery

2. **Retry Attempts**
   - Average number of retries per operation
   - Operations requiring maximum retries
   - Retry success rate

3. **Circuit Breaker State**
   - Number of times circuit opened
   - Duration circuit remained open
   - Success rate after half-open

4. **Partial Failures**
   - Percentage of batch operations with failures
   - Most common failure types
   - Files consistently failing

### Sample Monitoring Queries

```python
# Get failure rate
stats = checkpoint.get_statistics()
total = sum(s['count'] for s in stats['by_status'].values())
failed = stats['by_status'].get('failed', {}).get('count', 0)
failure_rate = (failed / total) * 100 if total > 0 else 0

# Alert if failure rate > 10%
if failure_rate > 10:
    send_alert(f"High failure rate: {failure_rate:.1f}%")
```

## Troubleshooting

### Common Issues

#### Issue: Files not being reprocessed after changes

**Cause**: Checkpoint exists with old checksum

**Solution**:
```bash
python scripts/recover_pipeline.py clear \
    --pipeline ingest \
    --scope file \
    --key "s3://bucket/file.parquet"
```

#### Issue: Transaction keeps rolling back

**Cause**: Validation failing or errors in transaction

**Solution**: Check validation hooks and error logs:
```python
# Add debug logging to validators
txn.add_validator(lambda key: debug_validate(key))
```

#### Issue: Circuit breaker stuck open

**Cause**: Underlying service issue not resolved

**Solution**: Check service health and manually reset:
```python
circuit_breaker.reset()
```

#### Issue: Database locked errors

**Cause**: Multiple processes accessing same checkpoint DB

**Solution**: Use separate checkpoint DBs per pipeline:
```python
checkpoint = PipelineCheckpoint(
    pipeline_name="ingest",
    db_path=".checkpoints/ingest_checkpoint.db"
)
```

## Performance Considerations

### Checkpoint Database Size

- Checkpoints are stored in DuckDB
- Regularly clean completed checkpoints
- Consider partitioning by time period for large-scale operations

### Retry Delays

- Initial delay: 1-2 seconds
- Maximum delay: 30-60 seconds
- Exponential base: 2.0 (doubles each retry)
- Use jitter to prevent thundering herd

### Circuit Breaker Thresholds

- Failure threshold: 5-10 failures
- Timeout: 30-120 seconds
- Adjust based on service characteristics

### Transaction Staging

- Staging area is cleaned up automatically
- Monitor staging bucket size
- Set lifecycle policies on staging prefix

## Summary

The OSAA pipeline error handling system provides:

- Robust idempotent operations
- Comprehensive retry logic
- Atomic transactions with rollback
- Detailed checkpoint tracking
- Manual recovery tools
- Clear error categorization

This ensures reliable, fault-tolerant data processing that can recover from failures and resume processing without data loss or corruption.
