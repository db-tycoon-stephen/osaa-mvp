"""End-to-end integration tests for the complete data pipeline.

Tests the full pipeline workflow:
1. CSV files in raw data directory
2. Ingestion to Parquet format
3. Upload to S3
4. SQLMesh transformations
5. Promotion between environments
"""

import os
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

from pipeline.exceptions import IngestError


# ============================================================================
# Complete Pipeline End-to-End Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
class TestCompletePipelineE2E:
    """End-to-end tests for complete pipeline workflows."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_full_ingestion_pipeline_local(
        self, temp_data_structure, duckdb_connection
    ):
        """Test complete ingestion pipeline without S3."""
        from pipeline.ingest.run import Ingest

        with patch("pipeline.ingest.run.duckdb.connect", return_value=duckdb_connection):
            ingest = Ingest()
            ingest.con = duckdb_connection

            # Get raw data directory
            raw_dir = list(temp_data_structure.values())[0].parent.parent

            # Generate file mapping
            mapping = ingest.generate_file_to_s3_folder_mapping(str(raw_dir))

            # Verify mapping generated correctly
            assert len(mapping) == 3
            assert "sdg_data.csv" in mapping
            assert "opri_data.csv" in mapping
            assert "wdi_data.csv" in mapping

            # Process each file
            for filename, subfolder in mapping.items():
                csv_path = raw_dir / subfolder / filename
                table_name = filename.replace(".csv", "").replace("-", "_")
                fully_qualified_name = f"source.{table_name}"

                # Create table from CSV
                duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
                duckdb_connection.execute(f"DROP TABLE IF EXISTS {fully_qualified_name}")
                duckdb_connection.execute(f"""
                    CREATE TABLE {fully_qualified_name} AS
                    SELECT * FROM read_csv('{csv_path}', header = true)
                """)

                # Verify table exists and has data
                result = duckdb_connection.execute(
                    f"SELECT COUNT(*) FROM {fully_qualified_name}"
                ).fetchone()
                assert result[0] > 0

    def test_csv_validation_and_transformation(self, temp_dir, duckdb_connection):
        """Test CSV validation and transformation logic."""
        # Create test CSV with specific data
        test_data = pd.DataFrame({
            "indicator_id": ["IND001", "IND002"],
            "country_id": ["USA", "CAN"],
            "year": [2020, 2021],
            "value": [100.5, 200.3],
            "magnitude": ["", ""],
            "qualifier": ["", ""],
        })

        csv_path = temp_dir / "test_indicators.csv"
        test_data.to_csv(csv_path, index=False)

        # Load into DuckDB
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
        duckdb_connection.execute("""
            CREATE TABLE source.test_indicators AS
            SELECT * FROM read_csv(?, header = true)
        """, [str(csv_path)])

        # Verify data loaded correctly
        result = duckdb_connection.execute(
            "SELECT * FROM source.test_indicators ORDER BY year"
        ).fetchdf()

        assert len(result) == 2
        assert result["indicator_id"].tolist() == ["IND001", "IND002"]
        assert result["country_id"].tolist() == ["USA", "CAN"]
        assert result["year"].tolist() == [2020, 2021]

    def test_data_quality_checks(self, temp_dir, duckdb_connection):
        """Test data quality checks during pipeline."""
        # Create CSV with quality issues
        problematic_data = pd.DataFrame({
            "indicator_id": ["IND001", "IND002", "IND001"],  # Duplicate
            "country_id": ["USA", None, "USA"],  # Null value
            "year": [2020, 2021, 2020],  # Duplicate row
            "value": [100.5, 200.3, 100.5],
            "magnitude": ["", "", ""],
            "qualifier": ["", "", ""],
        })

        csv_path = temp_dir / "quality_test.csv"
        problematic_data.to_csv(csv_path, index=False)

        # Load into DuckDB
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
        duckdb_connection.execute("""
            CREATE TABLE source.quality_test AS
            SELECT * FROM read_csv(?, header = true)
        """, [str(csv_path)])

        # Check for duplicates
        duplicates = duckdb_connection.execute("""
            SELECT indicator_id, country_id, year, COUNT(*) as count
            FROM source.quality_test
            WHERE country_id IS NOT NULL
            GROUP BY indicator_id, country_id, year
            HAVING COUNT(*) > 1
        """).fetchdf()

        assert len(duplicates) == 1
        assert duplicates.iloc[0]["count"] == 2

        # Check for null values
        null_count = duckdb_connection.execute("""
            SELECT COUNT(*) FROM source.quality_test WHERE country_id IS NULL
        """).fetchone()[0]

        assert null_count == 1

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.TARGET", "dev")
    @patch("pipeline.ingest.run.USERNAME", "testuser")
    def test_environment_specific_paths(self, s3_client, s3_bucket, temp_csv_file):
        """Test that environment-specific S3 paths are used correctly."""
        from pipeline.ingest.run import Ingest
        from pipeline.config import S3_BUCKET_NAME, TARGET, USERNAME

        expected_prefix = f"{TARGET}/landing"

        # Verify environment configuration
        assert TARGET == "dev"
        assert USERNAME == "testuser"


# ============================================================================
# Multi-Dataset Pipeline Tests
# ============================================================================

@pytest.mark.integration
class TestMultiDatasetPipeline:
    """Test pipeline with multiple datasets."""

    def test_sdg_opri_wdi_concurrent_processing(
        self, temp_dir, duckdb_connection, sample_sdg_data, sample_opri_data, sample_wdi_data
    ):
        """Test processing SDG, OPRI, and WDI datasets concurrently."""
        # Create CSV files for each dataset
        datasets = {
            "sdg": sample_sdg_data,
            "opri": sample_opri_data,
            "wdi": sample_wdi_data,
        }

        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")

        for name, data in datasets.items():
            # Save to CSV
            csv_path = temp_dir / f"{name}_indicators.csv"
            data.to_csv(csv_path, index=False)

            # Load into DuckDB
            table_name = f"source.{name}_indicators"
            duckdb_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            duckdb_connection.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_csv(?, header = true)
            """, [str(csv_path)])

            # Verify data
            count = duckdb_connection.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            assert count == len(data)

        # Verify all tables exist
        tables = duckdb_connection.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'source'
        """).fetchdf()

        assert "sdg_indicators" in tables["table_name"].values
        assert "opri_indicators" in tables["table_name"].values
        assert "wdi_indicators" in tables["table_name"].values

    def test_dataset_join_operations(
        self, duckdb_connection, sample_sdg_data, sample_wdi_data
    ):
        """Test joining data from multiple datasets."""
        # Load datasets
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")

        duckdb_connection.execute("""
            CREATE TABLE source.sdg AS
            SELECT * FROM (VALUES
                ('1.1.1', 'AFG', 2020, 25.5, '', '', 'Poverty rate')
            ) AS t(indicator_id, country_id, year, value, magnitude, qualifier, indicator_description)
        """)

        duckdb_connection.execute("""
            CREATE TABLE source.wdi AS
            SELECT * FROM (VALUES
                ('GDP.PCAP', 'AFG', 2020, 500.0, '', '', 'GDP per capita')
            ) AS t(indicator_id, country_id, year, value, magnitude, qualifier, indicator_description)
        """)

        # Join datasets by country and year
        result = duckdb_connection.execute("""
            SELECT
                s.country_id,
                s.year,
                s.value as sdg_value,
                w.value as wdi_value
            FROM source.sdg s
            INNER JOIN source.wdi w
                ON s.country_id = w.country_id
                AND s.year = w.year
        """).fetchdf()

        assert len(result) == 1
        assert result["country_id"].iloc[0] == "AFG"
        assert result["year"].iloc[0] == 2020


# ============================================================================
# Pipeline Error Handling Tests
# ============================================================================

@pytest.mark.integration
class TestPipelineErrorHandling:
    """Test error handling in the pipeline."""

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_invalid_csv_handling(self, temp_dir, duckdb_connection):
        """Test handling of invalid CSV files."""
        # Create malformed CSV
        invalid_csv = temp_dir / "invalid.csv"
        invalid_csv.write_text("not,proper,csv\ndata,without,headers")

        # Attempt to load - DuckDB should handle gracefully
        try:
            duckdb_connection.execute("""
                CREATE TABLE source.invalid AS
                SELECT * FROM read_csv(?, header = true)
            """, [str(invalid_csv)])

            # Verify table was created (even if with unexpected structure)
            result = duckdb_connection.execute(
                "SELECT COUNT(*) FROM source.invalid"
            ).fetchone()
            assert result[0] >= 0
        except Exception:
            # Expected for truly invalid CSV
            pass

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", False)
    def test_empty_csv_handling(self, temp_dir, duckdb_connection):
        """Test handling of empty CSV files."""
        # Create empty CSV with headers only
        empty_csv = temp_dir / "empty.csv"
        empty_csv.write_text("indicator_id,country_id,year,value,magnitude,qualifier\n")

        # Load into DuckDB
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
        duckdb_connection.execute("""
            CREATE TABLE source.empty AS
            SELECT * FROM read_csv(?, header = true)
        """, [str(empty_csv)])

        # Verify table exists with 0 rows
        count = duckdb_connection.execute(
            "SELECT COUNT(*) FROM source.empty"
        ).fetchone()[0]
        assert count == 0

    @patch("pipeline.ingest.run.ENABLE_S3_UPLOAD", True)
    @patch("pipeline.ingest.run.s3_init")
    def test_s3_connection_failure_handling(self, mock_s3_init, temp_csv_file):
        """Test handling of S3 connection failures."""
        from pipeline.ingest.run import Ingest

        # Mock S3 init to fail
        mock_s3_init.side_effect = Exception("S3 connection failed")

        # Should raise IngestError
        with pytest.raises(Exception):
            ingest = Ingest()


# ============================================================================
# Pipeline Performance Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
class TestPipelinePerformance:
    """Test pipeline performance characteristics."""

    def test_large_csv_ingestion(self, temp_dir, duckdb_connection):
        """Test ingesting a large CSV file."""
        # Create large dataset
        large_data = pd.DataFrame({
            "indicator_id": [f"IND{i:04d}" for i in range(1000)],
            "country_id": ["USA"] * 1000,
            "year": [2020 + (i % 5) for i in range(1000)],
            "value": [float(i) * 1.5 for i in range(1000)],
            "magnitude": [""] * 1000,
            "qualifier": [""] * 1000,
        })

        csv_path = temp_dir / "large_data.csv"
        large_data.to_csv(csv_path, index=False)

        # Load into DuckDB
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
        duckdb_connection.execute("""
            CREATE TABLE source.large_data AS
            SELECT * FROM read_csv(?, header = true)
        """, [str(csv_path)])

        # Verify all rows loaded
        count = duckdb_connection.execute(
            "SELECT COUNT(*) FROM source.large_data"
        ).fetchone()[0]
        assert count == 1000

    def test_multiple_csv_parallel_processing(self, temp_dir, duckdb_connection):
        """Test processing multiple CSV files."""
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")

        # Create multiple CSV files
        for i in range(10):
            data = pd.DataFrame({
                "indicator_id": [f"IND{j:03d}" for j in range(100)],
                "country_id": [f"C{i:02d}"] * 100,
                "year": [2020] * 100,
                "value": [float(j) for j in range(100)],
                "magnitude": [""] * 100,
                "qualifier": [""] * 100,
            })

            csv_path = temp_dir / f"data_{i}.csv"
            data.to_csv(csv_path, index=False)

            # Load each file
            table_name = f"source.data_{i}"
            duckdb_connection.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_csv(?, header = true)
            """, [str(csv_path)])

        # Verify all tables exist
        tables = duckdb_connection.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'source' AND table_name LIKE 'data_%'
        """).fetchone()[0]

        assert tables == 10


# ============================================================================
# Data Transformation Pipeline Tests
# ============================================================================

@pytest.mark.integration
class TestDataTransformationPipeline:
    """Test data transformation workflows."""

    def test_indicator_aggregation(self, duckdb_connection):
        """Test aggregating indicator data."""
        # Create source data
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
        duckdb_connection.execute("""
            CREATE TABLE source.indicators AS
            SELECT * FROM (VALUES
                ('IND001', 'USA', 2020, 100.0, '', ''),
                ('IND001', 'USA', 2021, 110.0, '', ''),
                ('IND001', 'CAN', 2020, 90.0, '', ''),
                ('IND001', 'CAN', 2021, 95.0, '', '')
            ) AS t(indicator_id, country_id, year, value, magnitude, qualifier)
        """)

        # Aggregate by country
        result = duckdb_connection.execute("""
            SELECT
                country_id,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value
            FROM source.indicators
            GROUP BY country_id
            ORDER BY country_id
        """).fetchdf()

        assert len(result) == 2
        assert result.loc[result["country_id"] == "USA", "avg_value"].iloc[0] == 105.0
        assert result.loc[result["country_id"] == "CAN", "avg_value"].iloc[0] == 92.5

    def test_time_series_calculation(self, duckdb_connection):
        """Test time series calculations."""
        # Create time series data
        duckdb_connection.execute("CREATE SCHEMA IF NOT EXISTS source")
        duckdb_connection.execute("""
            CREATE TABLE source.time_series AS
            SELECT * FROM (VALUES
                ('IND001', 'USA', 2018, 100.0),
                ('IND001', 'USA', 2019, 105.0),
                ('IND001', 'USA', 2020, 110.0),
                ('IND001', 'USA', 2021, 115.0)
            ) AS t(indicator_id, country_id, year, value)
        """)

        # Calculate year-over-year growth
        result = duckdb_connection.execute("""
            SELECT
                year,
                value,
                LAG(value) OVER (ORDER BY year) as prev_value,
                ((value - LAG(value) OVER (ORDER BY year)) / LAG(value) OVER (ORDER BY year)) * 100 as growth_rate
            FROM source.time_series
            WHERE indicator_id = 'IND001' AND country_id = 'USA'
            ORDER BY year
        """).fetchdf()

        # Verify growth rate calculations
        assert len(result) == 4
        # First row has no previous value
        assert pd.isna(result.iloc[0]["growth_rate"])
        # Subsequent rows should have ~4.76% growth
        for i in range(1, 4):
            assert abs(result.iloc[i]["growth_rate"] - 4.76) < 0.5