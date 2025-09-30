"""Unit tests for configuration module.

Tests cover:
- Environment variable loading
- Configuration validation
- AWS credential validation
- Directory creation
- S3 configuration
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pipeline.exceptions import ConfigurationError


# ============================================================================
# Environment Variable Tests
# ============================================================================

@pytest.mark.unit
class TestEnvironmentVariables:
    """Test environment variable configuration."""

    def test_target_default_value(self, monkeypatch):
        """Test TARGET defaults to 'dev'."""
        # Remove TARGET if it exists
        monkeypatch.delenv("TARGET", raising=False)

        # Import after env is set
        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.TARGET == "dev"

    def test_username_default_value(self, monkeypatch):
        """Test USERNAME defaults to 'default'."""
        monkeypatch.delenv("USERNAME", raising=False)

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.USERNAME == "default"

    def test_enable_s3_upload_default(self, monkeypatch):
        """Test ENABLE_S3_UPLOAD defaults to true."""
        monkeypatch.delenv("ENABLE_S3_UPLOAD", raising=False)

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.ENABLE_S3_UPLOAD is True

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("false", False),
            ("False", False),
            ("FALSE", False),
            ("anything_else", False),
        ],
    )
    def test_enable_s3_upload_values(self, value, expected, monkeypatch):
        """Test ENABLE_S3_UPLOAD parsing."""
        monkeypatch.setenv("ENABLE_S3_UPLOAD", value)

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.ENABLE_S3_UPLOAD == expected


# ============================================================================
# S3 Environment Path Tests
# ============================================================================

@pytest.mark.unit
class TestS3EnvPath:
    """Test S3 environment path construction."""

    def test_s3_env_prod(self, monkeypatch):
        """Test S3_ENV for prod environment."""
        monkeypatch.setenv("TARGET", "prod")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.S3_ENV == "prod"

    def test_s3_env_dev(self, monkeypatch):
        """Test S3_ENV for dev environment."""
        monkeypatch.setenv("TARGET", "dev")
        monkeypatch.setenv("USERNAME", "alice")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.S3_ENV == "dev/dev_alice"

    def test_s3_env_qa(self, monkeypatch):
        """Test S3_ENV for QA environment."""
        monkeypatch.setenv("TARGET", "qa")
        monkeypatch.setenv("USERNAME", "bob")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.S3_ENV == "dev/qa_bob"


# ============================================================================
# Directory Configuration Tests
# ============================================================================

@pytest.mark.unit
class TestDirectoryConfiguration:
    """Test directory configuration."""

    def test_root_dir_exists(self):
        """Test that ROOT_DIR is set correctly."""
        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                import pipeline.config as config_module

                assert config_module.ROOT_DIR is not None
                assert isinstance(config_module.ROOT_DIR, str)

    def test_data_directories_structure(self):
        """Test data directory structure."""
        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                import pipeline.config as config_module

                # Verify directory hierarchy
                assert config_module.DATALAKE_DIR.endswith("data")
                assert "raw" in config_module.RAW_DATA_DIR
                assert "staging" in config_module.STAGING_DATA_DIR
                assert "master" in config_module.MASTER_DATA_DIR


# ============================================================================
# Configuration Validation Tests
# ============================================================================

@pytest.mark.unit
class TestValidateConfig:
    """Test configuration validation."""

    @patch("os.makedirs")
    def test_validate_config_creates_directories(self, mock_makedirs, temp_dir, monkeypatch):
        """Test that validate_config creates required directories."""
        # Import here to use patched validate_config
        monkeypatch.setenv("ENABLE_S3_UPLOAD", "false")

        with patch("pipeline.config.ROOT_DIR", str(temp_dir)):
            from pipeline.config import validate_config

            validate_config()

            # Verify makedirs was called for required directories
            assert mock_makedirs.call_count >= 5

    @patch("os.makedirs", side_effect=OSError("Permission denied"))
    def test_validate_config_directory_creation_error(self, mock_makedirs, monkeypatch):
        """Test validate_config handles directory creation errors."""
        monkeypatch.setenv("ENABLE_S3_UPLOAD", "false")

        from pipeline.config import validate_config

        with pytest.raises(ConfigurationError, match="Unable to create directory"):
            validate_config()

    @patch("pipeline.config.S3_BUCKET_NAME", "")
    @patch("pipeline.config.ENABLE_S3_UPLOAD", True)
    @patch("os.makedirs")
    def test_validate_config_missing_s3_bucket(self, mock_makedirs, monkeypatch):
        """Test validate_config fails with missing S3 bucket name."""
        from pipeline.config import validate_config

        with pytest.raises(
            ConfigurationError, match="S3 upload is enabled but no bucket name"
        ):
            validate_config()

    @patch("os.makedirs")
    def test_validate_config_success(self, mock_makedirs, monkeypatch):
        """Test successful configuration validation."""
        monkeypatch.setenv("ENABLE_S3_UPLOAD", "false")
        monkeypatch.setenv("TARGET", "dev")

        from pipeline.config import validate_config

        # Should not raise
        validate_config()


# ============================================================================
# AWS Credentials Validation Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestValidateAWSCredentials:
    """Test AWS credentials validation."""

    @patch("boto3.client")
    def test_validate_aws_credentials_success(self, mock_boto_client, test_env_vars, monkeypatch):
        """Test successful AWS credentials validation."""
        # Set environment variables
        for key, value in test_env_vars.items():
            if key.startswith("AWS_"):
                monkeypatch.setenv(key, value)

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        from pipeline.config import validate_aws_credentials

        # Should not raise
        validate_aws_credentials()

        mock_s3.list_buckets.assert_called_once()

    def test_validate_aws_credentials_missing_access_key(self, monkeypatch):
        """Test validation fails with missing access key."""
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        from pipeline.config import validate_aws_credentials

        with pytest.raises(ConfigurationError, match="Missing AWS credential"):
            validate_aws_credentials()

    def test_validate_aws_credentials_missing_secret_key(self, monkeypatch):
        """Test validation fails with missing secret key."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST123456789")
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        from pipeline.config import validate_aws_credentials

        with pytest.raises(ConfigurationError, match="Missing AWS credential"):
            validate_aws_credentials()

    @patch("boto3.client")
    def test_validate_aws_credentials_invalid_format(self, mock_boto_client, monkeypatch):
        """Test validation fails with malformed credentials."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "short")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "tooshort")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        from pipeline.config import validate_aws_credentials

        with pytest.raises(ConfigurationError, match="Incomplete or malformed"):
            validate_aws_credentials()

    @patch("boto3.client")
    def test_validate_aws_credentials_invalid_access_key_id(
        self, mock_boto_client, monkeypatch
    ):
        """Test validation fails with invalid access key ID."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST1234567890")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "validsecretkey1234567890")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        # Mock S3 client to raise InvalidAccessKeyId error
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {
            "Error": {"Code": "InvalidAccessKeyId", "Message": "Invalid key"}
        }
        mock_s3.list_buckets.side_effect = ClientError(error_response, "list_buckets")

        from pipeline.config import validate_aws_credentials

        with pytest.raises(ConfigurationError, match="Invalid AWS Access Key"):
            validate_aws_credentials()

    @patch("boto3.client")
    def test_validate_aws_credentials_s3_access_denied(
        self, mock_boto_client, monkeypatch
    ):
        """Test validation fails with S3 access denied."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST1234567890")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "validsecretkey1234567890")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        # Mock S3 client to raise AccessDenied error
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {
            "Error": {"Code": "AccessDenied", "Message": "Access denied"}
        }
        mock_s3.list_buckets.side_effect = ClientError(error_response, "list_buckets")

        from pipeline.config import validate_aws_credentials

        with pytest.raises(ConfigurationError, match="S3 Access Failed"):
            validate_aws_credentials()


# ============================================================================
# Logger Tests
# ============================================================================

@pytest.mark.unit
class TestLogger:
    """Test logger creation."""

    def test_create_logger(self):
        """Test logger creation."""
        from pipeline.config import create_logger

        logger = create_logger()

        assert logger is not None
        assert logger.name == "pipeline.config"

    def test_logger_singleton(self):
        """Test that logger is a module-level singleton."""
        from pipeline.config import logger

        assert logger is not None


# ============================================================================
# S3 Folder Configuration Tests
# ============================================================================

@pytest.mark.unit
class TestS3FolderConfiguration:
    """Test S3 folder path configuration."""

    def test_landing_area_folder_prod(self, monkeypatch):
        """Test LANDING_AREA_FOLDER in prod."""
        monkeypatch.setenv("TARGET", "prod")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.LANDING_AREA_FOLDER == "prod/landing"

    def test_staging_area_folder_prod(self, monkeypatch):
        """Test STAGING_AREA_FOLDER in prod."""
        monkeypatch.setenv("TARGET", "prod")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.STAGING_AREA_FOLDER == "prod/staging"

    def test_landing_area_folder_dev(self, monkeypatch):
        """Test LANDING_AREA_FOLDER in dev."""
        monkeypatch.setenv("TARGET", "dev")
        monkeypatch.setenv("USERNAME", "testuser")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.LANDING_AREA_FOLDER == "dev/dev_testuser/landing"


# ============================================================================
# Edge Cases Tests
# ============================================================================

@pytest.mark.unit
class TestConfigEdgeCases:
    """Test edge cases in configuration."""

    def test_empty_target_environment(self, monkeypatch):
        """Test behavior with empty TARGET."""
        monkeypatch.setenv("TARGET", "")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                # Empty string should become empty after .lower()
                assert config_module.TARGET == ""

    def test_uppercase_target_normalized(self, monkeypatch):
        """Test that TARGET is normalized to lowercase."""
        monkeypatch.setenv("TARGET", "PROD")

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.TARGET == "prod"

    def test_custom_db_path(self, monkeypatch, temp_dir):
        """Test custom DB_PATH environment variable."""
        custom_path = str(temp_dir / "custom.db")
        monkeypatch.setenv("DB_PATH", custom_path)

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.DB_PATH == custom_path

    def test_custom_raw_data_dir(self, monkeypatch, temp_dir):
        """Test custom RAW_DATA_DIR environment variable."""
        custom_dir = str(temp_dir / "custom_raw")
        monkeypatch.setenv("RAW_DATA_DIR", custom_dir)

        with patch("pipeline.config.validate_config"):
            with patch("pipeline.config.validate_aws_credentials"):
                from importlib import reload
                import pipeline.config as config_module

                reload(config_module)

                assert config_module.RAW_DATA_DIR == custom_dir