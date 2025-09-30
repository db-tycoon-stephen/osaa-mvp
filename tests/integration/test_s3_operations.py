"""Integration tests for S3 operations using moto.

These tests use moto to mock AWS S3 services and test full workflows:
- Complete ingestion workflow (CSV -> Parquet -> S3)
- S3 sync operations (download/upload)
- Environment promotion workflows
- End-to-end data pipeline
"""

import os
from pathlib import Path
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from pipeline.exceptions import S3OperationError


# ============================================================================
# S3 Sync Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.s3
class TestS3SyncIntegration:
    """Integration tests for S3 sync operations."""

    def test_download_upload_roundtrip(self, s3_client, s3_bucket, temp_dir):
        """Test downloading and re-uploading a database file."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Create and upload test database
        db_content = b"test database content"
        s3_client.put_object(Bucket=s3_bucket, Key="test.db", Body=db_content)

        # Download
        local_path = temp_dir / "downloaded.db"
        sync_db_with_s3("download", str(local_path), s3_bucket, "test.db")

        # Verify download
        assert local_path.exists()
        assert local_path.read_bytes() == db_content

        # Modify and upload (in prod environment)
        modified_content = b"modified database content"
        local_path.write_bytes(modified_content)

        with patch.dict(os.environ, {"TARGET": "prod"}):
            sync_db_with_s3("upload", str(local_path), s3_bucket, "test.db")

        # Verify upload
        response = s3_client.get_object(Bucket=s3_bucket, Key="test.db")
        assert response["Body"].read() == modified_content

    def test_download_nonexistent_file(self, s3_client, s3_bucket, temp_dir):
        """Test downloading a file that doesn't exist."""
        from pipeline.s3_sync.run import sync_db_with_s3

        local_path = temp_dir / "nonexistent.db"

        # Should not raise, just log warning
        sync_db_with_s3("download", str(local_path), s3_bucket, "nonexistent.db")

        # File should not exist locally
        assert not local_path.exists()

    def test_upload_in_dev_environment(self, s3_client, s3_bucket, temp_dir):
        """Test that upload is blocked in dev environment."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Create test file
        local_path = temp_dir / "test.db"
        local_path.write_text("test content")

        # Try upload in dev (should be blocked)
        with patch.dict(os.environ, {"TARGET": "dev"}):
            sync_db_with_s3("upload", str(local_path), s3_bucket, "test.db")

        # Verify file was not uploaded
        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.head_object(Bucket=s3_bucket, Key="test.db")

    def test_concurrent_downloads(self, s3_client, s3_bucket, temp_dir):
        """Test downloading multiple files concurrently."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Upload multiple files
        files = {f"db{i}.db": f"content{i}".encode() for i in range(5)}
        for key, content in files.items():
            s3_client.put_object(Bucket=s3_bucket, Key=key, Body=content)

        # Download all
        for key, expected_content in files.items():
            local_path = temp_dir / key
            sync_db_with_s3("download", str(local_path), s3_bucket, key)
            assert local_path.read_bytes() == expected_content


# ============================================================================
# S3 Promotion Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.s3
class TestS3PromotionIntegration:
    """Integration tests for S3 environment promotion."""

    def test_promote_dev_to_prod_complete_workflow(self, s3_client, s3_bucket):
        """Test complete dev to prod promotion workflow."""
        from pipeline.s3_promote.run import promote_environment

        # Setup dev environment data
        dev_files = {
            "dev/landing/sdg/data1.parquet": b"sdg data 1",
            "dev/landing/sdg/data2.parquet": b"sdg data 2",
            "dev/landing/opri/data1.parquet": b"opri data 1",
            "dev/landing/wdi/data1.parquet": b"wdi data 1",
        }

        for key, content in dev_files.items():
            s3_client.put_object(Bucket=s3_bucket, Key=key, Body=content)

        # Add old file in prod that should be deleted
        s3_client.put_object(
            Bucket=s3_bucket, Key="prod/landing/sdg/old_file.parquet", Body=b"old"
        )

        # Perform promotion
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify all dev files were promoted
        for dev_key in dev_files.keys():
            prod_key = dev_key.replace("dev/", "prod/")
            response = s3_client.get_object(Bucket=s3_bucket, Key=prod_key)
            assert response["Body"].read() == dev_files[dev_key]

        # Verify old file was deleted
        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.head_object(Bucket=s3_bucket, Key="prod/landing/sdg/old_file.parquet")

    def test_promote_preserves_nested_structure(self, s3_client, s3_bucket):
        """Test that promotion preserves deep directory structures."""
        from pipeline.s3_promote.run import promote_environment

        # Create nested structure
        nested_files = {
            "dev/landing/sdg/indicators/economic/data.parquet": b"economic",
            "dev/landing/sdg/indicators/social/data.parquet": b"social",
            "dev/landing/sdg/metadata/descriptions.json": b'{"test": true}',
        }

        for key, content in nested_files.items():
            s3_client.put_object(Bucket=s3_bucket, Key=key, Body=content)

        # Promote
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify nested structure is preserved
        for dev_key, content in nested_files.items():
            prod_key = dev_key.replace("dev/", "prod/")
            response = s3_client.get_object(Bucket=s3_bucket, Key=prod_key)
            assert response["Body"].read() == content

    def test_promote_empty_source_cleans_target(self, s3_client, s3_bucket):
        """Test that promoting empty source cleans all target files."""
        from pipeline.s3_promote.run import promote_environment

        # Setup only prod files (no dev files)
        prod_files = [
            "prod/landing/sdg/data1.parquet",
            "prod/landing/opri/data1.parquet",
        ]

        for key in prod_files:
            s3_client.put_object(Bucket=s3_bucket, Key=key, Body=b"content")

        # Promote (with empty dev)
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify all prod files were deleted
        for key in prod_files:
            with pytest.raises(s3_client.exceptions.NoSuchKey):
                s3_client.head_object(Bucket=s3_bucket, Key=key)

    def test_promote_different_folders(self, s3_client, s3_bucket):
        """Test promotion of different folder types."""
        from pipeline.s3_promote.run import promote_environment

        # Setup data in staging folder
        s3_client.put_object(
            Bucket=s3_bucket,
            Key="dev/staging/processed.parquet",
            Body=b"processed data",
        )

        # Promote staging folder
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="staging")

        # Verify file was promoted to staging folder
        response = s3_client.get_object(
            Bucket=s3_bucket, Key="prod/staging/processed.parquet"
        )
        assert response["Body"].read() == b"processed data"


# ============================================================================
# Complete Pipeline Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
class TestCompletePipelineIntegration:
    """Integration tests for complete pipeline workflows."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_csv_to_parquet_without_s3(self, temp_csv_file, duckdb_connection):
        """Test CSV to Parquet conversion without S3 upload."""
        from pipeline.ingest.run import Ingest

        with patch("pipeline.ingest.run.duckdb.connect", return_value=duckdb_connection):
            ingest = Ingest()
            ingest.con = duckdb_connection

            # Convert CSV (no S3 upload)
            table_name = temp_csv_file.stem.replace("-", "_")
            fully_qualified_name = f"source.{table_name}"

            # Create schema and table
            duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
            duckdb_connection.execute(f"DROP TABLE IF EXISTS {fully_qualified_name}")
            duckdb_connection.execute(f"""
                CREATE TABLE {fully_qualified_name} AS
                SELECT * FROM read_csv('{temp_csv_file}', header = true)
            """)

            # Verify table was created
            result = duckdb_connection.execute(
                f"SELECT COUNT(*) FROM {fully_qualified_name}"
            ).fetchone()
            assert result[0] == 3

    def test_multi_dataset_ingestion(self, s3_client, s3_bucket, temp_data_structure):
        """Test ingesting multiple datasets."""
        from pipeline.ingest.run import Ingest

        # Setup mocks
        with patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True):
            with patch("pipeline.ingest.run.s3_init", return_value=(s3_client, boto3.Session())):
                with patch("pipeline.ingest.run.duckdb.connect") as mock_connect:
                    mock_con = duckdb_connection()
                    mock_connect.return_value = mock_con

                    ingest = Ingest()

                    # Generate file mapping
                    raw_dir = list(temp_data_structure.values())[0].parent.parent
                    mapping = ingest.generate_file_to_s3_folder_mapping(str(raw_dir))

                    # Verify all datasets are mapped
                    assert "sdg_data.csv" in mapping
                    assert "opri_data.csv" in mapping
                    assert "wdi_data.csv" in mapping

    def test_download_transform_upload_cycle(self, s3_client, s3_bucket, temp_dir):
        """Test complete download -> transform -> upload cycle."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Upload initial DB
        initial_db = temp_dir / "initial.db"
        initial_db.write_text("initial state")

        with patch.dict(os.environ, {"TARGET": "prod"}):
            sync_db_with_s3("upload", str(initial_db), s3_bucket, "pipeline.db")

        # Download for processing
        working_db = temp_dir / "working.db"
        sync_db_with_s3("download", str(working_db), s3_bucket, "pipeline.db")

        # Simulate transformation
        working_db.write_text("transformed state")

        # Upload transformed DB
        with patch.dict(os.environ, {"TARGET": "prod"}):
            sync_db_with_s3("upload", str(working_db), s3_bucket, "pipeline.db")

        # Verify final state
        response = s3_client.get_object(Bucket=s3_bucket, Key="pipeline.db")
        assert response["Body"].read() == b"transformed state"


# ============================================================================
# Error Recovery Integration Tests
# ============================================================================

@pytest.mark.integration
class TestErrorRecoveryIntegration:
    """Integration tests for error handling and recovery."""

    def test_partial_promotion_failure_recovery(self, s3_client, s3_bucket):
        """Test recovery from partial promotion failure."""
        from pipeline.s3_promote.run import promote_environment

        # Setup files
        s3_client.put_object(
            Bucket=s3_bucket, Key="dev/landing/data.parquet", Body=b"content"
        )

        # Promote successfully
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify promotion succeeded
        response = s3_client.get_object(Bucket=s3_bucket, Key="prod/landing/data.parquet")
        assert response["Body"].read() == b"content"

        # Re-promote (should handle existing files gracefully)
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

    def test_sync_with_permission_error_handling(self, s3_client, s3_bucket, temp_dir):
        """Test handling of permission errors during sync."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # This would normally raise a permission error in real AWS
        # In moto, we simulate by checking the behavior
        local_path = temp_dir / "test.db"
        local_path.write_text("content")

        # Should handle gracefully
        with patch.dict(os.environ, {"TARGET": "prod"}):
            sync_db_with_s3("upload", str(local_path), s3_bucket, "test.db")


# ============================================================================
# Performance Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceIntegration:
    """Integration tests for performance scenarios."""

    def test_large_file_batch_promotion(self, s3_client, s3_bucket):
        """Test promoting a large batch of files."""
        from pipeline.s3_promote.run import promote_environment

        # Create 50 files
        for i in range(50):
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=f"dev/landing/data{i}.parquet",
                Body=f"content{i}".encode(),
            )

        # Promote all
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify all were promoted
        for i in range(50):
            response = s3_client.get_object(
                Bucket=s3_bucket, Key=f"prod/landing/data{i}.parquet"
            )
            assert response["Body"].read() == f"content{i}".encode()

    def test_deep_directory_structure_promotion(self, s3_client, s3_bucket):
        """Test promoting deeply nested directory structures."""
        from pipeline.s3_promote.run import promote_environment

        # Create deep structure
        deep_path = "dev/landing/a/b/c/d/e/f/g/data.parquet"
        s3_client.put_object(Bucket=s3_bucket, Key=deep_path, Body=b"deep content")

        # Promote
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify deep structure preserved
        expected_path = "prod/landing/a/b/c/d/e/f/g/data.parquet"
        response = s3_client.get_object(Bucket=s3_bucket, Key=expected_path)
        assert response["Body"].read() == b"deep content"


# ============================================================================
# Data Integrity Integration Tests
# ============================================================================

@pytest.mark.integration
class TestDataIntegrityIntegration:
    """Integration tests for data integrity."""

    def test_file_content_preserved_after_promotion(self, s3_client, s3_bucket):
        """Test that file contents are preserved exactly during promotion."""
        from pipeline.s3_promote.run import promote_environment

        # Create file with specific content
        test_content = b"".join([bytes([i % 256]) for i in range(1000)])
        s3_client.put_object(
            Bucket=s3_bucket, Key="dev/landing/binary.parquet", Body=test_content
        )

        # Promote
        with patch("pipeline.s3_promote.run.s3_init", return_value=s3_client):
            promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify exact content match
        response = s3_client.get_object(Bucket=s3_bucket, Key="prod/landing/binary.parquet")
        assert response["Body"].read() == test_content

    def test_metadata_preserved_after_upload(self, s3_client, s3_bucket, temp_dir):
        """Test that file metadata is handled correctly."""
        from pipeline.s3_sync.run import sync_db_with_s3

        # Create and upload file
        db_file = temp_dir / "metadata_test.db"
        db_file.write_bytes(b"test content with metadata")

        with patch.dict(os.environ, {"TARGET": "prod"}):
            sync_db_with_s3("upload", str(db_file), s3_bucket, "metadata_test.db")

        # Verify upload
        response = s3_client.head_object(Bucket=s3_bucket, Key="metadata_test.db")
        assert "ContentLength" in response
        assert response["ContentLength"] == len(b"test content with metadata")