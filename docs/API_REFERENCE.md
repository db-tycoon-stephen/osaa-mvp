# OSAA Data Pipeline - API Reference

## Table of Contents

1. [Overview](#overview)
2. [Core Modules](#core-modules)
3. [Pipeline Module](#pipeline-module)
4. [Ingest Module](#ingest-module)
5. [S3 Sync Module](#s3-sync-module)
6. [S3 Promote Module](#s3-promote-module)
7. [Utility Modules](#utility-modules)
8. [Macros and Helpers](#macros-and-helpers)

## Overview

This document provides comprehensive API documentation for all Python modules in the OSAA Data Pipeline. Each module section includes purpose, classes, methods, parameters, return values, and usage examples.

## Core Modules

### pipeline.config

**Purpose**: Centralized configuration management for the entire pipeline.

#### Class: `Config`

Manages pipeline configuration using environment variables and defaults.

```python
from pipeline.config import Config

config = Config()
```

**Attributes**:

| Attribute | Type | Description | Default |
|-----------|------|-------------|---------|
| `env` | str | Environment (dev/qa/prod) | "dev" |
| `s3_bucket` | str | S3 bucket name | "osaa-mvp" |
| `aws_region` | str | AWS region | "us-east-1" |
| `username` | str | Developer username | None |
| `data_dir` | Path | Local data directory | "./data" |
| `models_dir` | Path | SQLMesh models directory | "./sqlMesh/models" |

**Methods**:

##### `get_s3_path(prefix: str, suffix: str = "") -> str`

Constructs S3 path based on environment.

**Parameters**:
- `prefix` (str): Path prefix (e.g., "landing", "staging")
- `suffix` (str, optional): Additional path components

**Returns**:
- str: Complete S3 path

**Example**:
```python
config = Config()
path = config.get_s3_path("landing", "sdg/data.parquet")
# Returns: "dev/landing/sdg/data.parquet"
```

##### `validate() -> bool`

Validates configuration completeness.

**Returns**:
- bool: True if configuration is valid

**Raises**:
- `ConfigurationError`: If required settings are missing

---

### pipeline.catalog

**Purpose**: Manages data catalog and file locations across environments.

#### Class: `DataCatalog`

Provides centralized access to data locations and metadata.

```python
from pipeline.catalog import DataCatalog

catalog = DataCatalog(config)
```

**Methods**:

##### `get_source_path(source: str, file_type: str = "parquet") -> str`

Gets path for source data files.

**Parameters**:
- `source` (str): Data source name (e.g., "sdg", "wdi")
- `file_type` (str): File extension (default: "parquet")

**Returns**:
- str: Full path to source file

##### `list_sources() -> List[str]`

Lists all available data sources.

**Returns**:
- List[str]: List of source names

**Example**:
```python
catalog = DataCatalog(config)
sources = catalog.list_sources()
# Returns: ["sdg", "wdi", "opri"]

path = catalog.get_source_path("sdg", "parquet")
# Returns: "s3://osaa-mvp/dev/landing/sdg/SDG_DATA.parquet"
```

##### `get_model_metadata(model_name: str) -> Dict[str, Any]`

Retrieves metadata for a SQLMesh model.

**Parameters**:
- `model_name` (str): Fully qualified model name

**Returns**:
- Dict: Model metadata including schema, columns, description

---

### pipeline.utils

**Purpose**: Utility functions used across the pipeline.

#### Functions

##### `setup_logging(level: str = "INFO") -> logging.Logger`

Configures logging for the pipeline.

**Parameters**:
- `level` (str): Logging level (DEBUG, INFO, WARNING, ERROR)

**Returns**:
- logging.Logger: Configured logger instance

##### `retry(max_attempts: int = 3, delay: float = 1.0)`

Decorator for retrying failed operations.

**Parameters**:
- `max_attempts` (int): Maximum retry attempts
- `delay` (float): Delay between retries in seconds

**Example**:
```python
@retry(max_attempts=5, delay=2.0)
def upload_to_s3(file_path, bucket, key):
    # Upload logic here
    pass
```

##### `measure_time(func: Callable) -> Callable`

Decorator to measure function execution time.

**Example**:
```python
@measure_time
def process_data(df):
    # Processing logic
    return transformed_df
```

##### `format_bytes(size: int) -> str`

Formats byte size to human-readable format.

**Parameters**:
- `size` (int): Size in bytes

**Returns**:
- str: Formatted size (e.g., "1.5 GB")

## Pipeline Module

### pipeline.__init__

**Purpose**: Main pipeline orchestration module.

#### Class: `Pipeline`

Main pipeline orchestration class.

```python
from pipeline import Pipeline

pipeline = Pipeline(config)
```

**Methods**:

##### `run(mode: str = "full") -> bool`

Executes the pipeline in specified mode.

**Parameters**:
- `mode` (str): Execution mode ("full", "ingest", "transform", "upload")

**Returns**:
- bool: True if successful

**Example**:
```python
pipeline = Pipeline(config)
success = pipeline.run(mode="full")
```

##### `validate_environment() -> bool`

Validates pipeline environment and dependencies.

**Returns**:
- bool: True if environment is valid

##### `get_status() -> Dict[str, Any]`

Gets current pipeline status.

**Returns**:
- Dict: Status information including last run, errors, metrics

## Ingest Module

### pipeline.ingest.run

**Purpose**: Handles data ingestion from various sources to S3.

#### Class: `DataIngester`

Manages data ingestion process.

```python
from pipeline.ingest.run import DataIngester

ingester = DataIngester(config)
```

**Methods**:

##### `ingest_csv(file_path: str, source: str, schema: Dict[str, str]) -> bool`

Ingests CSV file and converts to Parquet.

**Parameters**:
- `file_path` (str): Path to CSV file
- `source` (str): Source identifier
- `schema` (Dict[str, str]): Column schema mapping

**Returns**:
- bool: True if successful

**Example**:
```python
schema = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int",
    "value": "Decimal"
}

ingester = DataIngester(config)
success = ingester.ingest_csv(
    "data/raw/sdg/SDG_DATA.csv",
    "sdg",
    schema
)
```

##### `ingest_all_sources() -> Dict[str, bool]`

Ingests all configured data sources.

**Returns**:
- Dict[str, bool]: Success status for each source

##### `validate_schema(df: pd.DataFrame, schema: Dict[str, str]) -> bool`

Validates DataFrame against expected schema.

**Parameters**:
- `df` (pd.DataFrame): DataFrame to validate
- `schema` (Dict[str, str]): Expected schema

**Returns**:
- bool: True if schema matches

**Raises**:
- `SchemaValidationError`: If schema doesn't match

## S3 Sync Module

### pipeline.s3_sync.run

**Purpose**: Synchronizes local files with S3 storage.

#### Class: `S3Synchronizer`

Handles bi-directional sync between local and S3.

```python
from pipeline.s3_sync.run import S3Synchronizer

syncer = S3Synchronizer(config)
```

**Methods**:

##### `sync_to_s3(local_path: str, s3_prefix: str, exclude: List[str] = None) -> int`

Syncs local directory to S3.

**Parameters**:
- `local_path` (str): Local directory path
- `s3_prefix` (str): S3 prefix for upload
- `exclude` (List[str], optional): Patterns to exclude

**Returns**:
- int: Number of files uploaded

**Example**:
```python
syncer = S3Synchronizer(config)
count = syncer.sync_to_s3(
    "data/staging",
    "dev/staging",
    exclude=["*.tmp", "*.log"]
)
print(f"Uploaded {count} files")
```

##### `sync_from_s3(s3_prefix: str, local_path: str) -> int`

Syncs S3 directory to local.

**Parameters**:
- `s3_prefix` (str): S3 prefix to download
- `local_path` (str): Local directory path

**Returns**:
- int: Number of files downloaded

##### `calculate_diff(local_path: str, s3_prefix: str) -> Dict[str, List[str]]`

Calculates differences between local and S3.

**Returns**:
- Dict with keys: "local_only", "s3_only", "modified"

## S3 Promote Module

### pipeline.s3_promote.run

**Purpose**: Promotes data between environments (dev → qa → prod).

#### Class: `DataPromoter`

Manages data promotion across environments.

```python
from pipeline.s3_promote.run import DataPromoter

promoter = DataPromoter(config)
```

**Methods**:

##### `promote(source_env: str, target_env: str, dry_run: bool = False) -> Dict[str, Any]`

Promotes data between environments.

**Parameters**:
- `source_env` (str): Source environment ("dev", "qa")
- `target_env` (str): Target environment ("qa", "prod")
- `dry_run` (bool): If True, only simulate promotion

**Returns**:
- Dict: Promotion results including files copied, size

**Example**:
```python
promoter = DataPromoter(config)

# Dry run first
result = promoter.promote("dev", "qa", dry_run=True)
print(f"Would promote {result['file_count']} files")

# Actual promotion
result = promoter.promote("dev", "qa", dry_run=False)
print(f"Promoted {result['file_count']} files ({result['total_size']} bytes)")
```

##### `validate_promotion(source_env: str, target_env: str) -> bool`

Validates if promotion is allowed.

**Parameters**:
- `source_env` (str): Source environment
- `target_env` (str): Target environment

**Returns**:
- bool: True if promotion is valid

**Raises**:
- `PromotionError`: If promotion path is invalid

##### `create_backup(env: str) -> str`

Creates backup before promotion.

**Parameters**:
- `env` (str): Environment to backup

**Returns**:
- str: Backup location

## Utility Modules

### pipeline.logging_config

**Purpose**: Centralized logging configuration.

#### Functions

##### `get_logger(name: str, level: str = "INFO") -> logging.Logger`

Gets configured logger instance.

**Parameters**:
- `name` (str): Logger name
- `level` (str): Logging level

**Returns**:
- logging.Logger: Configured logger

**Example**:
```python
from pipeline.logging_config import get_logger

logger = get_logger(__name__, "DEBUG")
logger.info("Processing started")
```

### pipeline.exceptions

**Purpose**: Custom exception definitions.

#### Exception Classes

##### `PipelineError`

Base exception for pipeline errors.

```python
class PipelineError(Exception):
    """Base exception for pipeline errors"""
    pass
```

##### `ConfigurationError(PipelineError)`

Raised when configuration is invalid.

##### `IngestionError(PipelineError)`

Raised when data ingestion fails.

##### `TransformationError(PipelineError)`

Raised when transformation fails.

##### `S3Error(PipelineError)`

Raised when S3 operations fail.

**Example**:
```python
from pipeline.exceptions import IngestionError

def ingest_data(file_path):
    if not os.path.exists(file_path):
        raise IngestionError(f"File not found: {file_path}")
```

### pipeline.error_handler

**Purpose**: Centralized error handling and recovery.

#### Class: `ErrorHandler`

Manages error handling and recovery strategies.

```python
from pipeline.error_handler import ErrorHandler

handler = ErrorHandler()
```

**Methods**:

##### `handle_error(error: Exception, context: Dict[str, Any]) -> bool`

Handles errors with appropriate recovery strategy.

**Parameters**:
- `error` (Exception): The error that occurred
- `context` (Dict): Context information about the error

**Returns**:
- bool: True if error was recovered

**Example**:
```python
handler = ErrorHandler()

try:
    process_data()
except Exception as e:
    context = {"operation": "data_processing", "file": "data.csv"}
    if not handler.handle_error(e, context):
        raise
```

## Macros and Helpers

### macros.ibis_expressions

**Purpose**: Ibis expression generation utilities for SQLMesh.

#### Functions

##### `generate_ibis_table(evaluator, table_name, column_schema, schema_name)`

Generates Ibis table expression for SQLMesh models.

**Parameters**:
- `evaluator` (MacroEvaluator): SQLMesh macro evaluator
- `table_name` (str): Name of the table
- `column_schema` (Dict[str, str]): Column schema definition
- `schema_name` (str): Database schema name

**Returns**:
- ibis.Table: Ibis table expression

**Example**:
```python
from macros.ibis_expressions import generate_ibis_table

@model("sources.example")
def entrypoint(evaluator: MacroEvaluator) -> str:
    table = generate_ibis_table(
        evaluator,
        table_name="raw_data",
        column_schema={"id": "Int", "name": "String"},
        schema_name="sources"
    )
    return ibis.to_sql(table)
```

### macros.utils

**Purpose**: Utility functions for SQLMesh models.

#### Functions

##### `get_sql_model_schema(evaluator, model_name, source_folder_path)`

Retrieves schema for SQL model.

**Parameters**:
- `evaluator` (MacroEvaluator): SQLMesh macro evaluator
- `model_name` (str): Name of the model
- `source_folder_path` (str): Path to source folder

**Returns**:
- Dict[str, str]: Column schema

##### `find_indicator_models() -> List[Tuple[str, str]]`

Finds all indicator models in the project.

**Returns**:
- List[Tuple[str, str]]: List of (source_name, module_path) tuples

**Example**:
```python
from macros.utils import find_indicator_models

models = find_indicator_models()
for source, module_path in models:
    print(f"Found model: {source} at {module_path}")
```

## Error Codes

| Code | Error | Description | Resolution |
|------|-------|-------------|------------|
| E001 | ConfigurationError | Missing required configuration | Check .env file |
| E002 | S3AccessError | Cannot access S3 bucket | Verify AWS credentials |
| E003 | SchemaValidationError | Data doesn't match schema | Check source data format |
| E004 | TransformationError | SQLMesh transformation failed | Review model SQL |
| E005 | PromotionError | Invalid promotion path | Check environment sequence |
| E006 | DuckDBError | Database operation failed | Check disk space/locks |
| E007 | NetworkError | Network connection failed | Check connectivity |
| E008 | TimeoutError | Operation timed out | Increase timeout or retry |

## Usage Examples

### Complete Pipeline Run

```python
from pipeline import Pipeline
from pipeline.config import Config
from pipeline.logging_config import get_logger

# Setup
logger = get_logger(__name__)
config = Config()

# Initialize pipeline
pipeline = Pipeline(config)

# Validate environment
if not pipeline.validate_environment():
    logger.error("Environment validation failed")
    exit(1)

# Run full pipeline
try:
    logger.info("Starting pipeline execution")
    success = pipeline.run(mode="full")

    if success:
        logger.info("Pipeline completed successfully")
    else:
        logger.error("Pipeline failed")

except Exception as e:
    logger.exception(f"Pipeline error: {e}")
    exit(1)
```

### Custom Data Ingestion

```python
from pipeline.ingest.run import DataIngester
from pipeline.config import Config

config = Config()
ingester = DataIngester(config)

# Define custom schema
schema = {
    "date": "Date",
    "country": "String",
    "metric": "String",
    "value": "Float64"
}

# Ingest custom data
success = ingester.ingest_csv(
    file_path="data/custom/metrics.csv",
    source="custom_metrics",
    schema=schema
)

if success:
    print("Custom data ingested successfully")
```

### Environment Promotion with Validation

```python
from pipeline.s3_promote.run import DataPromoter
from pipeline.config import Config

config = Config()
promoter = DataPromoter(config)

# Validate promotion path
if not promoter.validate_promotion("dev", "prod"):
    print("ERROR: Cannot promote directly from dev to prod")
    exit(1)

# Create backup
backup_location = promoter.create_backup("qa")
print(f"Backup created at: {backup_location}")

# Perform promotion
try:
    result = promoter.promote("qa", "prod", dry_run=False)
    print(f"Promotion complete: {result}")
except Exception as e:
    print(f"Promotion failed: {e}")
    # Restore from backup if needed
```

## Testing

### Unit Test Example

```python
import unittest
from unittest.mock import Mock, patch
from pipeline.config import Config

class TestConfig(unittest.TestCase):

    @patch.dict('os.environ', {'ENVIRONMENT': 'test', 'S3_BUCKET': 'test-bucket'})
    def test_config_initialization(self):
        config = Config()
        self.assertEqual(config.env, 'test')
        self.assertEqual(config.s3_bucket, 'test-bucket')

    def test_get_s3_path(self):
        config = Config()
        config.env = 'dev'
        path = config.get_s3_path('landing', 'data.parquet')
        self.assertEqual(path, 'dev/landing/data.parquet')
```

### Integration Test Example

```python
import pytest
from pipeline import Pipeline
from pipeline.config import Config

@pytest.fixture
def test_config():
    config = Config()
    config.env = 'test'
    return config

def test_pipeline_integration(test_config):
    pipeline = Pipeline(test_config)

    # Test environment validation
    assert pipeline.validate_environment()

    # Test pipeline execution
    result = pipeline.run(mode="test")
    assert result is True
```

## Performance Considerations

### Memory Management

```python
# Use chunking for large files
def process_large_file(file_path, chunk_size=10000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        process_chunk(chunk)
```

### Connection Pooling

```python
# Reuse S3 client
class S3Manager:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            cls._client = boto3.client('s3')
        return cls._client
```

### Caching

```python
from functools import lru_cache

@lru_cache(maxsize=128)
def get_model_metadata(model_name):
    # Expensive metadata retrieval
    return load_metadata(model_name)
```

## Best Practices

1. **Always use configuration objects** instead of hardcoded values
2. **Handle errors gracefully** with appropriate logging
3. **Use type hints** for better code documentation
4. **Write tests** for all public methods
5. **Document edge cases** in docstrings
6. **Use context managers** for resource management
7. **Implement retries** for network operations
8. **Monitor memory usage** for large datasets
9. **Use async operations** where possible
10. **Version your APIs** for backward compatibility

---

*Last Updated: 2025-10-02*
*Version: 1.0.0*