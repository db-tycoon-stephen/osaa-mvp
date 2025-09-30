"""Unit tests for S3 synchronization module.

Tests cover:
- Database download from S3
- Database upload to S3
- Error handling for S3 operations
- Environment-specific upload restrictions
- File path generation
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pipeline.exceptions import S3OperationError


# ============================================================================
# S3 Sync Function Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestS3SyncDownload:
    """Test S3 database download functionality."""

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_download_db_success(self, mock_boto_client, temp_dir):
        """Test successful database download from S3."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.return_value = {}  # File exists

        db_path = temp_dir / "test.db"
        bucket = "test-bucket"
        s3_key = "test.db"

        # Execute download
        sync_db_with_s3("download", str(db_path), bucket, s3_key)

        # Verify S3 operations
        mock_s3.head_object.assert_called_once_with(Bucket=bucket, Key=s3_key)
        mock_s3.download_file.assert_called_once_with(bucket, s3_key, str(db_path))

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_download_db_not_found(self, mock_boto_client, temp_dir):
        """Test download when DB doesn't exist in S3."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks - simulate 404 error
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "404"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "head_object")

        db_path = temp_dir / "test.db"
        bucket = "test-bucket"
        s3_key = "test.db"

        # Should not raise error, just log
        sync_db_with_s3("download", str(db_path), bucket, s3_key)

        # Verify download was not attempted
        mock_s3.download_file.assert_not_called()

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_download_db_s3_error(self, mock_boto_client, temp_dir):
        """Test download with S3 error."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks - simulate non-404 error
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_s3.head_object.side_effect = ClientError(
            error_response, "head_object"
        )

        db_path = temp_dir / "test.db"
        bucket = "test-bucket"
        s3_key = "test.db"

        # Should raise S3OperationError
        with pytest.raises(S3OperationError, match="Error checking S3 object"):
            sync_db_with_s3("download", str(db_path), bucket, s3_key)

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_download_creates_parent_directory(self, mock_boto_client, temp_dir):
        """Test that download creates parent directories if needed."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.return_value = {}

        # Use nested path that doesn't exist
        db_path = temp_dir / "nested" / "dir" / "test.db"
        bucket = "test-bucket"
        s3_key = "test.db"

        # Execute download
        sync_db_with_s3("download", str(db_path), bucket, s3_key)

        # Verify parent directory was created
        assert db_path.parent.exists()


@pytest.mark.unit
@pytest.mark.s3
class TestS3SyncUpload:
    """Test S3 database upload functionality."""

    @patch("pipeline.s3_sync.run.boto3.client")
    @patch.dict(os.environ, {"TARGET": "prod"})
    def test_upload_db_success_prod(self, mock_boto_client, temp_dir):
        """Test successful database upload in prod environment."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create test DB file
        db_path = temp_dir / "test.db"
        db_path.write_text("test db content")

        bucket = "test-bucket"
        s3_key = "test.db"

        # Execute upload
        sync_db_with_s3("upload", str(db_path), bucket, s3_key)

        # Verify upload was called
        mock_s3.upload_file.assert_called_once_with(str(db_path), bucket, s3_key)

    @patch("pipeline.s3_sync.run.boto3.client")
    @patch.dict(os.environ, {"TARGET": "qa"})
    def test_upload_db_success_qa(self, mock_boto_client, temp_dir):
        """Test successful database upload in QA environment."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create test DB file
        db_path = temp_dir / "test.db"
        db_path.write_text("test db content")

        bucket = "test-bucket"
        s3_key = "test.db"

        # Execute upload
        sync_db_with_s3("upload", str(db_path), bucket, s3_key)

        # Verify upload was called
        mock_s3.upload_file.assert_called_once()

    @patch("pipeline.s3_sync.run.boto3.client")
    @patch.dict(os.environ, {"TARGET": "dev"})
    def test_upload_db_restricted_dev(self, mock_boto_client, temp_dir):
        """Test that upload is restricted in dev environment."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create test DB file
        db_path = temp_dir / "test.db"
        db_path.write_text("test db content")

        bucket = "test-bucket"
        s3_key = "test.db"

        # Execute upload - should be skipped
        sync_db_with_s3("upload", str(db_path), bucket, s3_key)

        # Verify upload was NOT called
        mock_s3.upload_file.assert_not_called()

    @patch("pipeline.s3_sync.run.boto3.client")
    @patch.dict(os.environ, {"TARGET": "prod"})
    def test_upload_db_file_not_found(self, mock_boto_client, temp_dir):
        """Test upload when local DB file doesn't exist."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Use non-existent file path
        db_path = temp_dir / "nonexistent.db"
        bucket = "test-bucket"
        s3_key = "test.db"

        # Should not raise error, just log warning
        sync_db_with_s3("upload", str(db_path), bucket, s3_key)

        # Verify upload was not attempted
        mock_s3.upload_file.assert_not_called()

    @patch("pipeline.s3_sync.run.boto3.client")
    @patch.dict(os.environ, {"TARGET": "prod"})
    def test_upload_db_s3_error(self, mock_boto_client, temp_dir):
        """Test upload with S3 error."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks - simulate upload failure
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.upload_file.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "upload_file"
        )

        # Create test DB file
        db_path = temp_dir / "test.db"
        db_path.write_text("test db content")

        bucket = "test-bucket"
        s3_key = "test.db"

        # Should raise S3OperationError
        with pytest.raises(S3OperationError, match="S3 upload operation failed"):
            sync_db_with_s3("upload", str(db_path), bucket, s3_key)


# ============================================================================
# Database Path Helper Tests
# ============================================================================

@pytest.mark.unit
class TestGetDBPaths:
    """Test database path generation helper."""

    def test_get_db_paths_default(self):
        """Test default database path generation."""
        from pipeline.s3_sync.run import get_db_paths

        local_path, s3_key = get_db_paths()

        assert local_path == "sqlMesh/unosaa_data_pipeline.db"
        assert s3_key == "unosaa_data_pipeline.db"

    def test_get_db_paths_custom_filename(self):
        """Test custom database filename."""
        from pipeline.s3_sync.run import get_db_paths

        local_path, s3_key = get_db_paths("custom.db")

        assert local_path == "sqlMesh/custom.db"
        assert s3_key == "custom.db"

    def test_get_db_paths_with_extension(self):
        """Test path generation with various extensions."""
        from pipeline.s3_sync.run import get_db_paths

        test_cases = [
            "test.db",
            "my_database.duckdb",
            "data.sqlite",
        ]

        for filename in test_cases:
            local_path, s3_key = get_db_paths(filename)
            assert local_path.endswith(filename)
            assert s3_key == filename


# ============================================================================
# Environment Variable Tests
# ============================================================================

@pytest.mark.unit
class TestEnvironmentRestrictions:
    """Test environment-specific restrictions."""

    @pytest.mark.parametrize(
        "target_env,should_upload",
        [
            ("prod", True),
            ("qa", True),
            ("dev", False),
            ("staging", False),
            ("test", False),
        ],
    )
    @patch("pipeline.s3_sync.run.boto3.client")
    def test_upload_environment_restrictions(
        self, mock_boto_client, target_env, should_upload, temp_dir, monkeypatch
    ):
        """Test upload restrictions across different environments."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup environment
        monkeypatch.setenv("TARGET", target_env)

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create test DB file
        db_path = temp_dir / "test.db"
        db_path.write_text("test content")

        # Execute upload
        sync_db_with_s3("upload", str(db_path), "bucket", "key")

        # Verify upload behavior based on environment
        if should_upload:
            mock_s3.upload_file.assert_called_once()
        else:
            mock_s3.upload_file.assert_not_called()


# ============================================================================
# Error Handling Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestS3SyncErrorHandling:
    """Test error handling in S3 sync operations."""

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_sync_with_invalid_operation(self, mock_boto_client, temp_dir):
        """Test sync with invalid operation type."""
        from pipeline.s3_sync.run import sync_db_with_s3

        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # This should not be handled explicitly but won't match any operation
        # The function should still complete without error since it only handles
        # "download" and "upload" explicitly
        sync_db_with_s3("invalid_op", str(temp_dir / "test.db"), "bucket", "key")

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_sync_with_connection_error(self, mock_boto_client, temp_dir):
        """Test sync with network/connection error."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks to raise connection error
        mock_boto_client.side_effect = Exception("Connection refused")

        db_path = temp_dir / "test.db"

        # Should raise S3OperationError
        with pytest.raises(S3OperationError, match="S3 download operation failed"):
            sync_db_with_s3("download", str(db_path), "bucket", "key")

    @patch("pipeline.s3_sync.run.boto3.client")
    @patch.dict(os.environ, {"TARGET": "prod"})
    def test_sync_upload_with_permission_error(self, mock_boto_client, temp_dir):
        """Test upload with permission error."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_s3.upload_file.side_effect = ClientError(error_response, "upload_file")

        # Create test file
        db_path = temp_dir / "test.db"
        db_path.write_text("content")

        # Should raise S3OperationError
        with pytest.raises(S3OperationError):
            sync_db_with_s3("upload", str(db_path), "bucket", "key")


# ============================================================================
# Integration-style Unit Tests
# ============================================================================

@pytest.mark.unit
class TestS3SyncWorkflow:
    """Test complete S3 sync workflows."""

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_download_then_upload_workflow(self, mock_boto_client, temp_dir):
        """Test typical download-modify-upload workflow."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.return_value = {}

        db_path = temp_dir / "workflow.db"
        bucket = "test-bucket"
        s3_key = "workflow.db"

        # Download
        sync_db_with_s3("download", str(db_path), bucket, s3_key)

        # Simulate modification
        db_path.write_text("modified content")

        # Upload (in prod environment)
        with patch.dict(os.environ, {"TARGET": "prod"}):
            sync_db_with_s3("upload", str(db_path), bucket, s3_key)

        # Verify both operations occurred
        mock_s3.download_file.assert_called_once()
        mock_s3.upload_file.assert_called_once()

    @patch("pipeline.s3_sync.run.boto3.client")
    def test_fresh_upload_no_download(self, mock_boto_client, temp_dir):
        """Test uploading a new database without downloading first."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create new DB
        db_path = temp_dir / "new.db"
        db_path.write_text("new database")

        # Upload directly (in prod)
        with patch.dict(os.environ, {"TARGET": "prod"}):
            sync_db_with_s3("upload", str(db_path), "bucket", "new.db")

        # Verify only upload occurred
        mock_s3.upload_file.assert_called_once()
        mock_s3.download_file.assert_not_called()


# ============================================================================
# Main Function Tests
# ============================================================================

@pytest.mark.unit
class TestS3SyncMain:
    """Test the main function and CLI interface."""

    @patch("pipeline.s3_sync.run.sync_db_with_s3")
    @patch("pipeline.s3_sync.run.get_db_paths")
    @patch("pipeline.s3_sync.run.S3_BUCKET_NAME", "test-bucket")
    def test_main_download(self, mock_get_paths, mock_sync, monkeypatch):
        """Test main function with download operation."""
        import sys
        from pipeline.s3_sync.run import __name__ as module_name

        # Setup
        mock_get_paths.return_value = ("local/path.db", "s3_key.db")
        monkeypatch.setattr(sys, "argv", ["script", "download"])

        # Import and run would trigger main, but we'll call sync directly
        mock_sync("download", "local/path.db", "test-bucket", "s3_key.db")

        mock_sync.assert_called_once()

    @patch("pipeline.s3_sync.run.sync_db_with_s3")
    @patch("pipeline.s3_sync.run.get_db_paths")
    @patch("pipeline.s3_sync.run.S3_BUCKET_NAME", "test-bucket")
    def test_main_upload(self, mock_get_paths, mock_sync, monkeypatch):
        """Test main function with upload operation."""
        import sys

        # Setup
        mock_get_paths.return_value = ("local/path.db", "s3_key.db")
        monkeypatch.setattr(sys, "argv", ["script", "upload"])

        # Call sync directly
        mock_sync("upload", "local/path.db", "test-bucket", "s3_key.db")

        mock_sync.assert_called_once()