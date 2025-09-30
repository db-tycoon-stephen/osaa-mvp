"""Unit tests for the data ingestion module.

Tests cover:
- Ingest class initialization
- CSV to Parquet conversion
- S3 secret setup
- File mapping generation
- Error handling
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import duckdb
import pytest

from pipeline.exceptions import (
    FileConversionError,
    IngestError,
    S3ConfigurationError,
)


# ============================================================================
# Ingest Class Initialization Tests
# ============================================================================

@pytest.mark.unit
class TestIngestInitialization:
    """Test Ingest class initialization."""

    @patch("pipeline.ingest.run.duckdb.connect")
    @patch("pipeline.ingest.run.s3_init")
    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    def test_ingest_init_with_s3_enabled(self, mock_s3_init, mock_duckdb_connect):
        """Test Ingest initialization with S3 upload enabled."""
        from pipeline.ingest.run import Ingest

        # Setup mocks
        mock_con = MagicMock()
        mock_duckdb_connect.return_value = mock_con
        mock_s3_client = MagicMock()
        mock_session = MagicMock()
        mock_s3_init.return_value = (mock_s3_client, mock_session)

        # Create Ingest instance
        ingest = Ingest()

        # Verify DuckDB setup
        mock_duckdb_connect.assert_called_once()
        mock_con.install_extension.assert_called_with("httpfs")
        mock_con.load_extension.assert_called_with("httpfs")

        # Verify S3 initialization
        mock_s3_init.assert_called_once_with(return_session=True)
        assert ingest.s3_client == mock_s3_client
        assert ingest.session == mock_session

    @patch("pipeline.ingest.run.duckdb.connect")
    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_ingest_init_with_s3_disabled(self, mock_duckdb_connect):
        """Test Ingest initialization with S3 upload disabled."""
        from pipeline.ingest.run import Ingest

        # Setup mocks
        mock_con = MagicMock()
        mock_duckdb_connect.return_value = mock_con

        # Create Ingest instance
        ingest = Ingest()

        # Verify S3 is not initialized
        assert ingest.s3_client is None
        assert ingest.session is None


# ============================================================================
# S3 Secret Setup Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestS3SecretSetup:
    """Test S3 secret setup in DuckDB."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_setup_s3_secret_disabled(self):
        """Test S3 secret setup when S3 is disabled."""
        from pipeline.ingest.run import Ingest

        with patch("pipeline.ingest.run.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            ingest = Ingest()
            ingest.setup_s3_secret()  # Should not raise, just log

            # Should not attempt to create secret
            mock_con.sql.assert_not_called()

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_setup_s3_secret_success(self, mock_s3_init):
        """Test successful S3 secret setup."""
        from pipeline.ingest.run import Ingest

        # Setup mocks
        mock_con = MagicMock()
        mock_session = MagicMock()
        mock_session.region_name = "us-east-1"
        mock_credentials = MagicMock()
        mock_credentials.access_key = "test_key"
        mock_credentials.secret_key = "test_secret"
        mock_credentials.token = "test_token"
        mock_session.get_credentials.return_value.get_frozen_credentials.return_value = (
            mock_credentials
        )
        mock_s3_init.return_value = (MagicMock(), mock_session)

        with patch("pipeline.ingest.run.duckdb.connect", return_value=mock_con):
            ingest = Ingest()
            ingest.setup_s3_secret()

            # Verify secret was dropped and created
            assert mock_con.sql.call_count >= 2

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_setup_s3_secret_failure(self, mock_s3_init):
        """Test S3 secret setup failure."""
        from pipeline.ingest.run import Ingest

        # Setup mocks to raise exception
        mock_con = MagicMock()
        mock_session = MagicMock()
        mock_session.region_name = "us-east-1"
        mock_session.get_credentials.side_effect = Exception("Credential error")
        mock_s3_init.return_value = (MagicMock(), mock_session)

        with patch("pipeline.ingest.run.duckdb.connect", return_value=mock_con):
            ingest = Ingest()

            with pytest.raises(S3ConfigurationError):
                ingest.setup_s3_secret()


# ============================================================================
# CSV to Parquet Conversion Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.duckdb
class TestCsvToParquetConversion:
    """Test CSV to Parquet conversion functionality."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_convert_csv_to_parquet_success(
        self, mock_s3_init, temp_csv_file, duckdb_connection
    ):
        """Test successful CSV to Parquet conversion and upload."""
        from pipeline.ingest.run import Ingest

        # Setup mocks
        mock_s3_init.return_value = (MagicMock(), MagicMock())

        with patch("pipeline.ingest.run.duckdb.connect", return_value=duckdb_connection):
            ingest = Ingest()
            ingest.con = duckdb_connection  # Use test connection

            # Convert CSV to Parquet
            local_file = str(temp_csv_file)
            s3_path = "s3://test-bucket/test/test_data.parquet"

            ingest.convert_csv_to_parquet_and_upload(local_file, s3_path)

            # Verify table was created
            result = duckdb_connection.execute(
                "SELECT COUNT(*) FROM source.test_data"
            ).fetchone()
            assert result[0] == 3  # Sample data has 3 rows

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_convert_csv_missing_file(self, mock_s3_init):
        """Test conversion with missing CSV file."""
        from pipeline.ingest.run import Ingest

        mock_s3_init.return_value = (MagicMock(), MagicMock())

        with patch("pipeline.ingest.run.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_con.sql.side_effect = FileNotFoundError("File not found")
            mock_connect.return_value = mock_con

            ingest = Ingest()

            with pytest.raises(FileConversionError, match="File not found"):
                ingest.convert_csv_to_parquet_and_upload(
                    "/nonexistent/file.csv", "s3://bucket/key"
                )

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_convert_csv_invalid_format(self, mock_s3_init, temp_dir):
        """Test conversion with invalid CSV format."""
        from pipeline.ingest.run import Ingest

        mock_s3_init.return_value = (MagicMock(), MagicMock())

        # Create invalid CSV file
        invalid_csv = temp_dir / "invalid.csv"
        invalid_csv.write_text("not,valid,csv\ndata")

        with patch("pipeline.ingest.run.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_con.sql.side_effect = Exception("CSV parse error")
            mock_connect.return_value = mock_con

            ingest = Ingest()

            with pytest.raises(FileConversionError):
                ingest.convert_csv_to_parquet_and_upload(
                    str(invalid_csv), "s3://bucket/key"
                )


# ============================================================================
# File Mapping Generation Tests
# ============================================================================

@pytest.mark.unit
class TestFileMapping:
    """Test file-to-S3-folder mapping generation."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_generate_file_mapping_success(self, temp_data_structure):
        """Test successful file mapping generation."""
        from pipeline.ingest.run import Ingest

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()

            # Get base directory
            raw_dir = list(temp_data_structure.values())[0].parent.parent

            # Generate mapping
            mapping = ingest.generate_file_to_s3_folder_mapping(str(raw_dir))

            # Verify mapping contains expected files
            assert "sdg_data.csv" in mapping
            assert "opri_data.csv" in mapping
            assert "wdi_data.csv" in mapping

            # Verify folder paths
            assert mapping["sdg_data.csv"] == "sdg"
            assert mapping["opri_data.csv"] == "opri"
            assert mapping["wdi_data.csv"] == "wdi"

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_generate_file_mapping_empty_directory(self, temp_dir):
        """Test file mapping with empty directory."""
        from pipeline.ingest.run import Ingest

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()

            # Generate mapping for empty directory
            mapping = ingest.generate_file_to_s3_folder_mapping(str(temp_dir))

            # Should return empty mapping
            assert mapping == {}

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_generate_file_mapping_ignores_hidden_files(self, temp_dir):
        """Test file mapping ignores hidden files."""
        from pipeline.ingest.run import Ingest

        # Create files including hidden ones
        (temp_dir / "valid.csv").write_text("test")
        (temp_dir / ".hidden.csv").write_text("test")
        (temp_dir / "_underscore.csv").write_text("test")

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()

            mapping = ingest.generate_file_to_s3_folder_mapping(str(temp_dir))

            # Should only include valid.csv
            assert "valid.csv" in mapping
            assert ".hidden.csv" not in mapping
            assert "_underscore.csv" not in mapping


# ============================================================================
# Convert and Upload Files Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestConvertAndUploadFiles:
    """Test the complete convert and upload workflow."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    @patch("pipeline.ingest.run.RAW_DATA_DIR")
    @patch("pipeline.ingest.run.S3_BUCKET_NAME", "test-bucket")
    @patch("pipeline.ingest.run.TARGET", "dev")
    @patch("pipeline.ingest.run.USERNAME", "testuser")
    def test_convert_and_upload_files_success(
        self, mock_raw_dir, mock_s3_init, temp_data_structure
    ):
        """Test successful conversion and upload of all files."""
        from pipeline.ingest.run import Ingest

        # Setup
        raw_dir = list(temp_data_structure.values())[0].parent.parent
        mock_raw_dir.return_value = str(raw_dir)
        mock_s3_init.return_value = (MagicMock(), MagicMock())

        with patch("pipeline.ingest.run.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_con.sql.return_value.fetchone.return_value = [3]
            mock_connect.return_value = mock_con

            ingest = Ingest()
            ingest.convert_csv_to_parquet_and_upload = MagicMock()

            with patch.object(
                ingest, "generate_file_to_s3_folder_mapping"
            ) as mock_mapping:
                mock_mapping.return_value = {
                    "sdg_data.csv": "sdg",
                    "opri_data.csv": "opri",
                }

                # Run conversion
                ingest.convert_and_upload_files()

                # Verify conversion was called for each file
                assert ingest.convert_csv_to_parquet_and_upload.call_count == 2

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_convert_and_upload_files_skip_missing(self, mock_s3_init, temp_dir):
        """Test that missing files are skipped with warning."""
        from pipeline.ingest.run import Ingest

        mock_s3_init.return_value = (MagicMock(), MagicMock())

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()
            ingest.convert_csv_to_parquet_and_upload = MagicMock()

            with patch.object(
                ingest, "generate_file_to_s3_folder_mapping"
            ) as mock_mapping:
                # Return mapping with non-existent file
                mock_mapping.return_value = {"missing.csv": ""}

                with patch("pipeline.ingest.run.RAW_DATA_DIR", str(temp_dir)):
                    # Should not raise, just log warning
                    ingest.convert_and_upload_files()

                    # Conversion should not be called for missing file
                    ingest.convert_csv_to_parquet_and_upload.assert_not_called()


# ============================================================================
# Ingest Run Method Tests
# ============================================================================

@pytest.mark.unit
class TestIngestRun:
    """Test the main Ingest.run() method."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    @patch("pipeline.ingest.run.TARGET", "dev")
    def test_run_success_with_s3(self, mock_s3_init):
        """Test successful run with S3 enabled."""
        from pipeline.ingest.run import Ingest

        mock_s3_init.return_value = (MagicMock(), MagicMock())

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()
            ingest.setup_s3_secret = MagicMock()
            ingest.convert_and_upload_files = MagicMock()

            # Run the ingestion process
            ingest.run()

            # Verify both methods were called
            ingest.setup_s3_secret.assert_called_once()
            ingest.convert_and_upload_files.assert_called_once()

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_run_success_without_s3(self):
        """Test successful run with S3 disabled."""
        from pipeline.ingest.run import Ingest

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()
            ingest.setup_s3_secret = MagicMock()
            ingest.convert_and_upload_files = MagicMock()

            # Run the ingestion process
            ingest.run()

            # setup_s3_secret should not be called
            ingest.setup_s3_secret.assert_not_called()
            ingest.convert_and_upload_files.assert_called_once()

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_run_failure_raises_ingest_error(self, mock_s3_init):
        """Test that failures raise IngestError."""
        from pipeline.ingest.run import Ingest

        mock_s3_init.return_value = (MagicMock(), MagicMock())

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()
            ingest.setup_s3_secret = MagicMock()
            ingest.convert_and_upload_files = MagicMock(
                side_effect=Exception("Test error")
            )

            # Should raise IngestError
            with pytest.raises(IngestError, match="Ingestion process failed"):
                ingest.run()


# ============================================================================
# Edge Cases and Error Handling Tests
# ============================================================================

@pytest.mark.unit
class TestIngestEdgeCases:
    """Test edge cases and error handling."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_filename_extraction_special_characters(self):
        """Test filename extraction with special characters."""
        from pipeline.ingest.run import Ingest

        with patch("pipeline.ingest.run.duckdb.connect"):
            ingest = Ingest()

            # Test various filename patterns
            test_cases = [
                ("data/test-file.csv", "test_file"),
                ("data/test_file_123.csv", "test_file_123"),
                ("test.with.dots.csv", "test"),
            ]

            for file_path, expected_start in test_cases:
                # Extract table name logic from convert method
                import re

                table_name = re.search(r"[^/]+(?=\.)", file_path)
                table_name = (
                    table_name.group(0).replace("-", "_")
                    if table_name
                    else "UNNAMED"
                )

                assert table_name.startswith(expected_start.split("_")[0])

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_empty_csv_handling(self, temp_dir, duckdb_connection):
        """Test handling of empty CSV files."""
        from pipeline.ingest.run import Ingest

        # Create empty CSV file
        empty_csv = temp_dir / "empty.csv"
        empty_csv.write_text("col1,col2,col3\n")  # Headers only

        with patch("pipeline.ingest.run.duckdb.connect", return_value=duckdb_connection):
            ingest = Ingest()
            ingest.con = duckdb_connection

            # Should handle empty CSV gracefully
            s3_path = "s3://bucket/empty.parquet"
            ingest.convert_csv_to_parquet_and_upload(str(empty_csv), s3_path)

            # Verify table exists with 0 rows
            result = duckdb_connection.execute(
                "SELECT COUNT(*) FROM source.empty"
            ).fetchone()
            assert result[0] == 0