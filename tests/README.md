# UN-OSAA Data Pipeline Testing Guide

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest

# Run with coverage
pytest --cov=src/pipeline --cov-report=html
```

## Test Organization

### Unit Tests (`tests/unit/`)
Fast, isolated tests for individual functions and classes. Each module has its own test file:
- `test_ingest.py` - Data ingestion tests
- `test_s3_sync.py` - S3 synchronization tests
- `test_s3_promote.py` - Environment promotion tests
- `test_config.py` - Configuration validation tests

### Integration Tests (`tests/integration/`)
End-to-end tests with mocked AWS services:
- `test_s3_operations.py` - Complete S3 workflows
- `test_pipeline_end_to_end.py` - Full pipeline execution

### SQLMesh Tests (`sqlMesh/tests/`)
Data model validation tests:
- `test_sdg_indicators.yaml` - SDG model tests
- `test_opri_indicators.yaml` - OPRI model tests
- `test_wdi_indicators.yaml` - WDI model tests

## Running Tests

### By Category
```bash
pytest -m unit              # Unit tests only
pytest -m integration       # Integration tests only
pytest -m s3                # S3-related tests
pytest -m sqlmesh           # SQLMesh tests
pytest -m "not slow"        # Skip slow tests
```

### By File
```bash
pytest tests/unit/test_ingest.py
pytest tests/integration/test_s3_operations.py
```

### By Test Function
```bash
pytest tests/unit/test_ingest.py::TestIngestInitialization::test_ingest_init_with_s3_enabled
```

### Parallel Execution
```bash
pytest -n auto              # Auto-detect CPU count
pytest -n 4                 # Use 4 workers
```

### With Coverage
```bash
pytest --cov=src/pipeline --cov-report=html
open htmlcov/index.html
```

## Test Fixtures

Common fixtures are defined in `conftest.py`:

### Database Fixtures
- `duckdb_connection` - In-memory DuckDB database
- `duckdb_with_test_schema` - DuckDB with source schema

### S3 Fixtures
- `s3_client` - Mocked boto3 S3 client
- `s3_bucket` - Test S3 bucket
- `s3_with_test_data` - S3 bucket with sample data

### Data Fixtures
- `sample_csv_data` - Sample CSV DataFrame
- `sample_sdg_data` - SDG indicator data
- `sample_opri_data` - OPRI indicator data
- `sample_wdi_data` - WDI indicator data

### File Fixtures
- `temp_dir` - Temporary directory
- `temp_csv_file` - Temporary CSV file
- `temp_data_structure` - Complete directory structure

## Writing New Tests

### Unit Test Template
```python
import pytest
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_my_function(duckdb_connection):
    """Test description following Arrange-Act-Assert pattern."""
    # Arrange
    expected = "expected_result"

    # Act
    result = my_function()

    # Assert
    assert result == expected
```

### Integration Test Template
```python
import pytest

@pytest.mark.integration
@pytest.mark.s3
def test_s3_workflow(s3_client, s3_bucket):
    """Test complete workflow with mocked AWS."""
    # Setup
    s3_client.put_object(Bucket=s3_bucket, Key="test.parquet", Body=b"data")

    # Execute
    result = perform_operation()

    # Verify
    assert result["success"] is True
```

### SQLMesh Test Template
```yaml
test_model_validation:
  model: sources.my_model
  description: Test description
  inputs:
    schema.table:
      rows:
        - id: "001"
          value: 100
  outputs:
    query: |
      SELECT COUNT(*) as count
      FROM sources.my_model
    rows:
      - count: 1
```

## Test Markers

Available pytest markers (defined in `pytest.ini`):
- `@pytest.mark.unit` - Fast unit tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.slow` - Slow-running tests
- `@pytest.mark.s3` - S3-related tests
- `@pytest.mark.duckdb` - DuckDB tests
- `@pytest.mark.sqlmesh` - SQLMesh tests
- `@pytest.mark.smoke` - Quick smoke tests

## Coverage Goals

- Overall coverage: >70%
- Unit tests: >80% of module code
- Integration tests: All critical paths
- SQLMesh tests: All data models

## Troubleshooting

### Import Errors
```bash
pip install -e .
```

### AWS Credential Errors
```bash
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
export AWS_DEFAULT_REGION=us-east-1
```

### DuckDB Extension Errors
```bash
rm -rf ~/.duckdb/
```

### Coverage Not Generated
```bash
pip install pytest-cov
pytest --cov=src/pipeline --cov-report=term-missing
```

## Best Practices

1. **Test Naming**: Use descriptive names that explain what is being tested
2. **Test Independence**: Each test should be independent and repeatable
3. **Test Speed**: Keep unit tests fast (<1 second each)
4. **Mock External Services**: Always mock S3, APIs, and external dependencies
5. **Test Edge Cases**: Include tests for error conditions and boundary cases
6. **Clear Assertions**: Use specific assertions with helpful error messages
7. **Documentation**: Add docstrings explaining complex test scenarios

## CI/CD Integration

Tests run automatically on:
- Pull requests
- Pushes to main branch
- Scheduled nightly builds

Required checks:
- All tests pass
- Coverage >70%
- No linting errors
- SQLMesh model tests pass

## Additional Resources

- [pytest documentation](https://docs.pytest.org/)
- [moto documentation](https://docs.getmoto.org/)
- [SQLMesh testing guide](https://sqlmesh.readthedocs.io/en/stable/concepts/tests/)
- [Coverage.py documentation](https://coverage.readthedocs.io/)