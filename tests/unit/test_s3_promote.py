"""Unit tests for S3 environment promotion module.

Tests cover:
- Promoting data between environments (dev to prod)
- Copying objects from source to target
- Deleting orphaned objects in target
- Error handling for S3 operations
- Different folder structures
"""

from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

from pipeline.exceptions import S3OperationError


# ============================================================================
# Promote Environment Function Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestPromoteEnvironment:
    """Test environment promotion functionality."""

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_success_basic(self, mock_s3_init):
        """Test successful basic promotion from dev to prod."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock source objects
        source_objects = [
            {"Key": "dev/landing/sdg/data1.parquet"},
            {"Key": "dev/landing/sdg/data2.parquet"},
        ]

        # Mock paginator for source listing
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": source_objects}]

        # Execute promotion
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify s3_init was called
        mock_s3_init.assert_called_once()

        # Verify objects were copied
        assert mock_s3_client.copy_object.call_count == 2

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_with_target_cleanup(self, mock_s3_init):
        """Test promotion removes orphaned files in target."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock source objects
        source_objects = [{"Key": "dev/landing/sdg/data1.parquet"}]

        # Mock target objects (includes orphaned file)
        target_objects = [
            {"Key": "prod/landing/sdg/data1.parquet"},
            {"Key": "prod/landing/sdg/old_data.parquet"},  # Should be deleted
        ]

        # Setup paginator to return different results per call
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.side_effect = [
            [{"Contents": source_objects}],  # First call for source
            [{"Contents": target_objects}],  # Second call for target
        ]

        # Execute promotion
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify copy was called for source object
        mock_s3_client.copy_object.assert_called_once()

        # Verify delete was called for orphaned object
        mock_s3_client.delete_object.assert_called_once()
        delete_call = mock_s3_client.delete_object.call_args
        assert "old_data.parquet" in delete_call[1]["Key"]

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_empty_source(self, mock_s3_init):
        """Test promotion with empty source environment."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock empty source
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.side_effect = [
            [{}],  # No Contents key means no objects
            [{}],  # Empty target too
        ]

        # Execute promotion - should not fail
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify no copy or delete operations
        mock_s3_client.copy_object.assert_not_called()
        mock_s3_client.delete_object.assert_not_called()

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_custom_folder(self, mock_s3_init):
        """Test promotion with custom folder."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock objects in custom folder
        source_objects = [{"Key": "dev/staging/processed_data.parquet"}]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": source_objects}]

        # Execute promotion with staging folder
        promote_environment(source_env="dev", target_env="prod", folder="staging")

        # Verify copy was called with correct paths
        copy_call = mock_s3_client.copy_object.call_args
        assert "staging" in copy_call[1]["Key"]

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_preserves_directory_structure(self, mock_s3_init):
        """Test that promotion preserves nested directory structure."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock nested source structure
        source_objects = [
            {"Key": "dev/landing/sdg/indicators/data1.parquet"},
            {"Key": "dev/landing/sdg/metadata/info.json"},
            {"Key": "dev/landing/opri/data.parquet"},
        ]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": source_objects}]

        # Execute promotion
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify all files were copied with correct structure
        assert mock_s3_client.copy_object.call_count == 3

        # Check that structure is preserved
        calls = mock_s3_client.copy_object.call_args_list
        target_keys = [call[1]["Key"] for call in calls]

        assert "prod/landing/sdg/indicators/data1.parquet" in target_keys
        assert "prod/landing/sdg/metadata/info.json" in target_keys
        assert "prod/landing/opri/data.parquet" in target_keys


# ============================================================================
# Error Handling Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestPromoteErrorHandling:
    """Test error handling in promotion operations."""

    @patch("pipeline.s3_promote.run.s3_init")
    def test_promote_s3_init_failure(self, mock_s3_init):
        """Test handling of S3 initialization failure."""
        from pipeline.s3_promote.run import promote_environment

        # Mock s3_init to raise error
        mock_s3_init.side_effect = Exception("Cannot initialize S3")

        # Should raise S3OperationError
        with pytest.raises(S3OperationError, match="Promotion failed"):
            promote_environment()

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_list_objects_error(self, mock_s3_init):
        """Test handling of list_objects error."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock paginator to raise error
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_paginator.paginate.side_effect = ClientError(
            error_response, "list_objects_v2"
        )

        # Should raise S3OperationError
        with pytest.raises(S3OperationError, match="AWS operation failed"):
            promote_environment()

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_copy_object_error(self, mock_s3_init):
        """Test handling of copy_object error."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock successful list but failed copy
        source_objects = [{"Key": "dev/landing/data.parquet"}]
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": source_objects}]

        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.copy_object.side_effect = ClientError(
            error_response, "copy_object"
        )

        # Should raise S3OperationError
        with pytest.raises(S3OperationError, match="AWS operation failed"):
            promote_environment()

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_delete_object_error(self, mock_s3_init):
        """Test handling of delete_object error."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock source and target objects
        source_objects = [{"Key": "dev/landing/data.parquet"}]
        target_objects = [{"Key": "prod/landing/orphan.parquet"}]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.side_effect = [
            [{"Contents": source_objects}],
            [{"Contents": target_objects}],
        ]

        # Mock delete to fail
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_s3_client.delete_object.side_effect = ClientError(
            error_response, "delete_object"
        )

        # Should raise S3OperationError
        with pytest.raises(S3OperationError, match="AWS operation failed"):
            promote_environment()


# ============================================================================
# Environment Combination Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestPromoteEnvironmentCombinations:
    """Test different environment combinations."""

    @pytest.mark.parametrize(
        "source_env,target_env,folder",
        [
            ("dev", "prod", "landing"),
            ("dev", "prod", "staging"),
            ("qa", "prod", "landing"),
            ("dev", "qa", "landing"),
            ("staging", "prod", "landing"),
        ],
    )
    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_various_environments(
        self, mock_s3_init, source_env, target_env, folder
    ):
        """Test promotion across various environment combinations."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock objects
        source_key = f"{source_env}/{folder}/data.parquet"
        source_objects = [{"Key": source_key}]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": source_objects}]

        # Execute promotion
        promote_environment(source_env=source_env, target_env=target_env, folder=folder)

        # Verify copy was called with correct target
        copy_call = mock_s3_client.copy_object.call_args
        expected_target = f"{target_env}/{folder}/data.parquet"
        assert copy_call[1]["Key"] == expected_target


# ============================================================================
# Pagination Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestPromotePagination:
    """Test handling of paginated S3 responses."""

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_with_multiple_pages(self, mock_s3_init):
        """Test promotion with paginated source listing."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock multiple pages of objects
        page1 = [
            {"Key": "dev/landing/data1.parquet"},
            {"Key": "dev/landing/data2.parquet"},
        ]
        page2 = [
            {"Key": "dev/landing/data3.parquet"},
            {"Key": "dev/landing/data4.parquet"},
        ]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        # First call returns 2 pages, second call returns empty for target
        mock_paginator.paginate.side_effect = [
            [{"Contents": page1}, {"Contents": page2}],
            [{}],
        ]

        # Execute promotion
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify all 4 objects were copied
        assert mock_s3_client.copy_object.call_count == 4

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_large_batch(self, mock_s3_init):
        """Test promotion with large number of files."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Create 100 mock objects
        source_objects = [
            {"Key": f"dev/landing/data{i}.parquet"} for i in range(100)
        ]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": source_objects}]

        # Execute promotion
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify all 100 objects were copied
        assert mock_s3_client.copy_object.call_count == 100


# ============================================================================
# Main Function Tests
# ============================================================================

@pytest.mark.unit
class TestPromoteMain:
    """Test the main function."""

    @patch("pipeline.s3_promote.run.promote_environment")
    def test_main_success(self, mock_promote):
        """Test successful main function execution."""
        from pipeline.s3_promote.run import main

        # Execute main
        main()

        # Verify promote_environment was called
        mock_promote.assert_called_once()

    @patch("pipeline.s3_promote.run.promote_environment")
    def test_main_with_error(self, mock_promote):
        """Test main function with error."""
        from pipeline.s3_promote.run import main

        # Mock promote_environment to raise error
        mock_promote.side_effect = Exception("Promotion failed")

        # Should exit with code 1
        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1


# ============================================================================
# Edge Cases Tests
# ============================================================================

@pytest.mark.unit
@pytest.mark.s3
class TestPromoteEdgeCases:
    """Test edge cases in promotion."""

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_with_special_characters_in_keys(self, mock_s3_init):
        """Test promotion with special characters in S3 keys."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock objects with special characters
        source_objects = [
            {"Key": "dev/landing/data with spaces.parquet"},
            {"Key": "dev/landing/data-with-dashes.parquet"},
            {"Key": "dev/landing/data_with_underscores.parquet"},
        ]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": source_objects}]

        # Should not raise error
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Verify all files were copied
        assert mock_s3_client.copy_object.call_count == 3

    @patch("pipeline.s3_promote.run.s3_init")
    @patch("pipeline.s3_promote.run.S3_BUCKET_NAME", "test-bucket")
    def test_promote_identical_source_and_target(self, mock_s3_init):
        """Test promotion when source and target are identical."""
        from pipeline.s3_promote.run import promote_environment

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_init.return_value = mock_s3_client

        # Mock identical objects in both source and target
        objects = [{"Key": "dev/landing/data.parquet"}]

        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.side_effect = [
            [{"Contents": objects}],
            [{"Contents": [{"Key": "prod/landing/data.parquet"}]}],
        ]

        # Execute promotion
        promote_environment(source_env="dev", target_env="prod", folder="landing")

        # Should still copy (S3 will overwrite)
        mock_s3_client.copy_object.assert_called_once()

        # Should not delete anything (target file exists in source)
        mock_s3_client.delete_object.assert_not_called()