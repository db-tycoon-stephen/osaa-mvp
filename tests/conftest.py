"""Pytest configuration and shared fixtures for UN-OSAA Data Pipeline tests.

This module provides fixtures for:
- DuckDB database connections
- Mock AWS S3 services using moto
- Test data generation
- Temporary file management
- Environment configuration
"""

import os
import tempfile
from pathlib import Path
from typing import Dict, Generator
from unittest.mock import MagicMock, patch

import boto3
import duckdb
import pandas as pd
import pytest
from moto import mock_aws


# ============================================================================
# Environment and Configuration Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def test_env_vars() -> Dict[str, str]:
    """Provide test environment variables.

    Returns:
        Dictionary of environment variables for testing
    """
    return {
        "TARGET": "dev",
        "USERNAME": "testuser",
        "ENABLE_S3_UPLOAD": "true",
        "S3_BUCKET_NAME": "test-unosaa-data-pipeline",
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/test-role",
    }


@pytest.fixture(scope="function")
def mock_env(test_env_vars: Dict[str, str], monkeypatch) -> None:
    """Mock environment variables for testing.

    Args:
        test_env_vars: Dictionary of test environment variables
        monkeypatch: pytest monkeypatch fixture
    """
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(scope="function")
def mock_config_validation(monkeypatch):
    """Mock config validation to avoid AWS credential checks during import."""
    monkeypatch.setattr("pipeline.config.validate_config", lambda: None)
    monkeypatch.setattr("pipeline.config.validate_aws_credentials", lambda: None)


# ============================================================================
# DuckDB Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Provide an in-memory DuckDB connection for testing.

    Yields:
        DuckDB connection object
    """
    con = duckdb.connect(":memory:")

    # Install and load required extensions
    con.install_extension("httpfs")
    con.load_extension("httpfs")

    yield con

    con.close()


@pytest.fixture(scope="function")
def duckdb_with_test_schema(duckdb_connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Provide DuckDB connection with test schema created.

    Args:
        duckdb_connection: Base DuckDB connection

    Returns:
        DuckDB connection with source schema
    """
    duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
    return duckdb_connection


# ============================================================================
# AWS S3 Mocking Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def aws_credentials(test_env_vars: Dict[str, str], monkeypatch):
    """Mock AWS credentials for moto.

    Args:
        test_env_vars: Test environment variables
        monkeypatch: pytest monkeypatch fixture
    """
    for key, value in test_env_vars.items():
        if key.startswith("AWS_"):
            monkeypatch.setenv(key, value)


@pytest.fixture(scope="function")
def s3_mock(aws_credentials):
    """Provide mocked S3 service using moto.

    Yields:
        Mocked AWS context
    """
    with mock_aws():
        yield


@pytest.fixture(scope="function")
def s3_client(s3_mock, test_env_vars: Dict[str, str]):
    """Provide mocked S3 client.

    Args:
        s3_mock: Mocked AWS context
        test_env_vars: Test environment variables

    Returns:
        boto3 S3 client
    """
    return boto3.client("s3", region_name=test_env_vars["AWS_DEFAULT_REGION"])


@pytest.fixture(scope="function")
def s3_bucket(s3_client, test_env_vars: Dict[str, str]) -> str:
    """Create a test S3 bucket.

    Args:
        s3_client: boto3 S3 client
        test_env_vars: Test environment variables

    Returns:
        S3 bucket name
    """
    bucket_name = test_env_vars["S3_BUCKET_NAME"]
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture(scope="function")
def s3_with_test_data(s3_client, s3_bucket: str) -> Dict[str, str]:
    """Create S3 bucket with test data structure.

    Args:
        s3_client: boto3 S3 client
        s3_bucket: S3 bucket name

    Returns:
        Dictionary mapping test files to S3 keys
    """
    test_files = {
        "dev/landing/sdg/test_data.parquet": b"test_parquet_content",
        "dev/landing/opri/test_data.parquet": b"test_parquet_content",
        "dev/landing/wdi/test_data.parquet": b"test_parquet_content",
        "prod/landing/sdg/test_data.parquet": b"test_parquet_content",
    }

    for key, content in test_files.items():
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=content)

    return test_files


@pytest.fixture(scope="function")
def mock_sts_client(s3_mock, monkeypatch):
    """Mock STS assume_role for testing.

    Args:
        s3_mock: Mocked AWS context
        monkeypatch: pytest monkeypatch fixture
    """
    mock_credentials = {
        "Credentials": {
            "AccessKeyId": "mock_access_key",
            "SecretAccessKey": "mock_secret_key",
            "SessionToken": "mock_session_token",
            "Expiration": "2025-12-31T23:59:59Z",
        }
    }

    with patch("boto3.client") as mock_client:
        sts_mock = MagicMock()
        sts_mock.assume_role.return_value = mock_credentials
        mock_client.return_value = sts_mock
        yield sts_mock


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def sample_csv_data() -> pd.DataFrame:
    """Generate sample CSV data for testing.

    Returns:
        Pandas DataFrame with test data
    """
    return pd.DataFrame({
        "indicator_id": ["IND001", "IND002", "IND003"],
        "country_id": ["USA", "CAN", "MEX"],
        "year": [2020, 2021, 2022],
        "value": [100.5, 200.3, 300.7],
        "magnitude": ["", "", ""],
        "qualifier": ["", "", ""],
    })


@pytest.fixture(scope="function")
def sample_sdg_data() -> pd.DataFrame:
    """Generate sample SDG indicator data.

    Returns:
        Pandas DataFrame with SDG test data
    """
    return pd.DataFrame({
        "indicator_id": ["1.1.1", "1.2.1", "2.1.1"],
        "country_id": ["AFG", "BDI", "KEN"],
        "year": [2020, 2020, 2020],
        "value": [25.5, 30.2, 15.8],
        "magnitude": ["", "", ""],
        "qualifier": ["", "", ""],
        "indicator_description": [
            "Poverty headcount ratio",
            "Poverty rate",
            "Malnutrition rate",
        ],
    })


@pytest.fixture(scope="function")
def sample_opri_data() -> pd.DataFrame:
    """Generate sample OPRI indicator data.

    Returns:
        Pandas DataFrame with OPRI test data
    """
    return pd.DataFrame({
        "indicator_id": ["OPRI_001", "OPRI_002"],
        "country_id": ["NGA", "ETH"],
        "year": [2020, 2021],
        "value": [45.0, 52.3],
        "magnitude": ["", ""],
        "qualifier": ["", ""],
        "indicator_description": ["Peace index", "Conflict index"],
    })


@pytest.fixture(scope="function")
def sample_wdi_data() -> pd.DataFrame:
    """Generate sample WDI indicator data.

    Returns:
        Pandas DataFrame with WDI test data
    """
    return pd.DataFrame({
        "country_id": ["USA", "CAN"],
        "indicator_id": ["GDP.PCAP.CD", "NY.GDP.MKTP.CD"],
        "year": [2020, 2021],
        "value": [65298.0, 52051.0],
        "magnitude": ["", ""],
        "qualifier": ["", ""],
        "indicator_description": ["GDP per capita", "GDP total"],
    })


# ============================================================================
# Temporary File Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def temp_dir() -> Generator[Path, None, None]:
    """Provide a temporary directory for testing.

    Yields:
        Path to temporary directory
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture(scope="function")
def temp_csv_file(temp_dir: Path, sample_csv_data: pd.DataFrame) -> Path:
    """Create a temporary CSV file with test data.

    Args:
        temp_dir: Temporary directory path
        sample_csv_data: Sample CSV data

    Returns:
        Path to temporary CSV file
    """
    csv_path = temp_dir / "test_data.csv"
    sample_csv_data.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture(scope="function")
def temp_data_structure(temp_dir: Path, sample_csv_data: pd.DataFrame) -> Dict[str, Path]:
    """Create a temporary directory structure with test data files.

    Args:
        temp_dir: Temporary directory path
        sample_csv_data: Sample CSV data

    Returns:
        Dictionary mapping dataset names to file paths
    """
    structure = {}

    # Create subdirectories
    for dataset in ["sdg", "opri", "wdi"]:
        dataset_dir = temp_dir / dataset
        dataset_dir.mkdir(exist_ok=True)

        # Create CSV file
        csv_path = dataset_dir / f"{dataset}_data.csv"
        sample_csv_data.to_csv(csv_path, index=False)
        structure[dataset] = csv_path

    return structure


@pytest.fixture(scope="function")
def mock_raw_data_dir(temp_data_structure: Dict[str, Path], monkeypatch) -> Path:
    """Mock RAW_DATA_DIR with temporary directory structure.

    Args:
        temp_data_structure: Temporary data structure
        monkeypatch: pytest monkeypatch fixture

    Returns:
        Path to mocked raw data directory
    """
    raw_dir = list(temp_data_structure.values())[0].parent.parent
    monkeypatch.setattr("pipeline.config.RAW_DATA_DIR", str(raw_dir))
    return raw_dir


# ============================================================================
# Mock Fixtures for Pipeline Components
# ============================================================================

@pytest.fixture(scope="function")
def mock_logger():
    """Provide a mock logger for testing.

    Returns:
        Mock logger object
    """
    logger = MagicMock()
    logger.info = MagicMock()
    logger.error = MagicMock()
    logger.warning = MagicMock()
    logger.debug = MagicMock()
    return logger


@pytest.fixture(scope="function")
def mock_s3_init(s3_client, monkeypatch):
    """Mock the s3_init function to return test S3 client.

    Args:
        s3_client: Test S3 client
        monkeypatch: pytest monkeypatch fixture
    """
    def mock_init(return_session=False):
        session = boto3.Session(region_name="us-east-1")
        if return_session:
            return s3_client, session
        return s3_client

    monkeypatch.setattr("pipeline.utils.s3_init", mock_init)
    monkeypatch.setattr("pipeline.ingest.run.s3_init", mock_init)
    monkeypatch.setattr("pipeline.s3_promote.run.s3_init", mock_init)


# ============================================================================
# SQLMesh Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def sqlmesh_context(temp_dir: Path):
    """Provide SQLMesh context for testing models.

    Args:
        temp_dir: Temporary directory for SQLMesh state

    Returns:
        SQLMesh context
    """
    # Import here to avoid import issues
    try:
        from sqlmesh import Context

        config_path = temp_dir / "config.yaml"
        config_path.write_text("""
gateways:
  local:
    connection:
      type: duckdb
      database: ':memory:'

default_gateway: local
model_defaults:
  dialect: duckdb
""")

        context = Context(paths=[str(temp_dir)])
        return context
    except ImportError:
        pytest.skip("SQLMesh not available")


# ============================================================================
# Parametrized Test Data
# ============================================================================

@pytest.fixture(params=["sdg", "opri", "wdi"])
def dataset_name(request):
    """Parametrize tests across different datasets.

    Args:
        request: pytest request object

    Returns:
        Dataset name
    """
    return request.param


@pytest.fixture(params=["dev", "prod"])
def environment(request):
    """Parametrize tests across different environments.

    Args:
        request: pytest request object

    Returns:
        Environment name
    """
    return request.param