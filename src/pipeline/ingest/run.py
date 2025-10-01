"""Ingest module for data processing and S3 upload.

This module handles the ingestion of CSV files, converting them to Parquet,
and optionally uploading them to S3 for the United Nations OSAA MVP project.
"""

import os
import re
import tempfile
import time

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import duckdb
from typing import Dict, Optional

from pipeline.config import (
    ENABLE_S3_UPLOAD,
    LANDING_AREA_FOLDER,
    RAW_DATA_DIR,
    S3_BUCKET_NAME,
    TARGET,
    USERNAME,
)
from pipeline.exceptions import (
    FileConversionError,
    IngestError,
    S3ConfigurationError,
    S3OperationError,
)
from pipeline.logging_config import create_logger, log_exception
from pipeline.utils import s3_init
from pipeline.monitoring import get_metrics, MetricsContext
from pipeline.execution_tracker import ExecutionTracker
from pipeline.alerting import get_alert_manager, AlertSeverity

# Initialize logger
logger = create_logger(__name__)

# Initialize monitoring components
metrics = get_metrics(environment=TARGET)
alert_manager = get_alert_manager()


class Ingest:
    """Manage the data ingestion process for the United Nations OSAA MVP project.

    This class handles the conversion of CSV files to Parquet format,
    manages database connections, and optionally uploads processed files
    to S3 storage. It provides methods for file processing, table creation,
    and data transformation.

    Key features:
    - Convert CSV files to Parquet
    - Create and manage database tables
    - Optionally upload processed files to S3
    - Handle environment-specific configurations
    """

    def __init__(self) -> None:
        """Initialize the IngestProcess with DuckDB connection and S3 session.

        Sets up a DuckDB connection and optionally initializes an S3 session
        based on the configuration settings.
        """
        logger.info("Initializing Ingest Process")

        # Initialize DuckDB with required extensions
        self.con = duckdb.connect()
        self.con.install_extension('httpfs')
        self.con.load_extension('httpfs')

        # Initialize execution tracker
        self.tracker = ExecutionTracker()
        self.total_rows_processed = 0
        self.files_processed = 0

        if ENABLE_S3_UPLOAD:
            logger.info("Initializing S3 client...")
            self.s3_client, self.session = s3_init(return_session=True)
            logger.info("S3 Client Initialized")
        else:
            logger.warning("S3 upload is disabled")
            self.s3_client = None
            self.session = None

    def setup_s3_secret(self):
        """
        Set up the S3 secret in DuckDB for S3 access using AWS credential chain.

        :raises S3ConfigurationError: If there are issues setting up S3 secret
        """
        if not ENABLE_S3_UPLOAD:
            logger.info("S3 upload disabled, skipping S3 secret setup")
            return

        try:
            logger.info("🔐 Setting up S3 Secret in DuckDB")
            logger.info("   Creating S3 secret with assumed credentials")

            region = self.session.region_name
            credentials = self.session.get_credentials().get_frozen_credentials()
            logger.info(f"   Using AWS region: {region}")

            # Drop existing secret if it exists
            self.con.sql("DROP SECRET IF EXISTS my_s3_secret")
            logger.info("   Dropped existing S3 secret")

            # Create the SQL statement
            sql_statement = f"""
                CREATE PERSISTENT SECRET my_s3_secret (
                    TYPE S3,
                    KEY_ID '{credentials.access_key}',
                    SECRET '{credentials.secret_key}',
                    SESSION_TOKEN '{credentials.token}',
                    REGION '{region}'
                );
            """
            self.con.sql(sql_statement)
            logger.info("✅ S3 secret successfully created in DuckDB")

        except Exception as e:
            error_msg = f"AWS Credentials Error: {e}"
            log_exception(logger, e, {"context": "S3 secret setup"})
            raise S3ConfigurationError(error_msg)

    def convert_csv_to_parquet_and_upload(
        self, local_file_path: str, s3_file_path: Optional[str] = None
    ) -> None:
        """Convert a CSV file to Parquet and optionally upload it to S3.

        :param local_file_path: Path to the local CSV file
        :param s3_file_path: S3 path to upload the Parquet file
        :raises FileConversionError: If file conversion or upload fails
        """
        from datetime import datetime

        start_time = datetime.utcnow()
        row_count = 0

        try:
            # Extract table name from filename
            table_name = re.search(r"[^/]+(?=\.)", local_file_path)
            table_name = (
                table_name.group(0).replace("-", "_") if table_name else "UNNAMED"
            )
            fully_qualified_name = "source." + table_name
            logger.info(f"Processing file {local_file_path} into table {fully_qualified_name}")

            # Create schema and table
            logger.info("Creating schema and table...")
            self.con.sql("CREATE SCHEMA IF NOT EXISTS source")
            self.con.sql(f"DROP TABLE IF EXISTS {fully_qualified_name}")
            self.con.sql(
                f"""
                CREATE TABLE {fully_qualified_name} AS
                SELECT *
                FROM read_csv('{local_file_path}', header = true)
            """
            )
            logger.info(f"Successfully created table {fully_qualified_name}")

            # Verify table was created and has data
            try:
                row_count = self.con.sql(f"SELECT COUNT(*) FROM {fully_qualified_name}").fetchone()[0]
                logger.info(f"Table {fully_qualified_name} created with {row_count} rows")
                self.total_rows_processed += row_count
            except Exception as e:
                logger.error(f"Failed to get row count for table {fully_qualified_name}: {e}")
                raise FileConversionError(f"Failed to verify table creation: {e}")

            # Attempt S3 upload with timing
            upload_start = time.time()
            logger.info(f"Attempting to upload to S3: {s3_file_path}")
            copy_sql = f"""
                COPY (SELECT * FROM {fully_qualified_name})
                TO '{s3_file_path}'
                (FORMAT PARQUET)
            """
            self.con.sql(copy_sql)
            upload_duration = time.time() - upload_start

            # Get file size for metrics
            file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)

            # Log S3 upload metrics
            metrics.log_s3_upload(
                file_path=s3_file_path,
                file_size_mb=file_size_mb,
                upload_duration=upload_duration,
                status="success"
            )

            # Track model execution
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            self.tracker.track_model_execution(
                model_name=table_name,
                model_type="INGEST",
                start_time=start_time,
                end_time=end_time,
                status="success",
                rows_produced=row_count,
                bytes_written=int(file_size_mb * 1024 * 1024)
            )

            self.files_processed += 1

            logger.info(
                f"Successfully converted and uploaded {local_file_path} to {s3_file_path} "
                f"({row_count} rows in {duration:.2f}s)"
            )

        except FileNotFoundError as e:
            logger.error(f"File not found error: {e}")
            metrics.log_error("FileNotFoundError", str(e), {"file": local_file_path})
            raise FileConversionError(str(e))
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            metrics.log_error(type(e).__name__, str(e), {"file": local_file_path})
            raise FileConversionError(f"Conversion failed: {e}")

    def generate_file_to_s3_folder_mapping(self, raw_data_dir: str) -> dict:
        """
        Generate mapping of local files to their respective S3 folders.

        Args:
            raw_data_dir: The base directory containing raw data subfolders.

        Returns:
            A dictionary where the key is the filename and the value is the subfolder name.
        """
        file_to_s3_folder_mapping: Dict[str, str] = {}

        # Pattern to exclude any folder/file name with symbols (like hidden files)
        symbols = r"^[~!@#$%^&*()_\-+={[}}|:;\"'<,>.?/]+"

        # Traverse the raw_data directory
        for subdir, _, files in os.walk(raw_data_dir):
            logger.info(f"Walking directory: {subdir}, found files: {files}")

            # Get the relative path of the current subdirectory
            rel_subdir = os.path.relpath(subdir, raw_data_dir)

            for file in files:
                # Skip files starting with symbols
                if not re.match(symbols, file) and file.endswith(".csv"):
                    file_to_s3_folder_mapping[file] = (
                        rel_subdir if rel_subdir != "." else ""
                    )

        logger.info(f"Generated file mapping: {file_to_s3_folder_mapping}")
        return file_to_s3_folder_mapping

    def convert_and_upload_files(self):
        """
        Convert CSV files to Parquet and optionally upload them to S3.
        """
        try:
            file_mapping = self.generate_file_to_s3_folder_mapping(RAW_DATA_DIR)
            for file_name_csv, s3_sub_folder in file_mapping.items():
                local_file_path = os.path.join(
                    RAW_DATA_DIR, s3_sub_folder, file_name_csv
                )

                # Convert filename to Parquet
                file_name_pq = f"{os.path.splitext(file_name_csv)[0]}.parquet"

                # Construct S3 path
                logger.info(f"Constructing S3 path with TARGET={TARGET}, USERNAME={USERNAME}")
                s3_file_path = f"s3://{S3_BUCKET_NAME}/{TARGET}/landing/{s3_sub_folder}/{file_name_pq}"
                
                logger.info(s3_file_path)

                if os.path.isfile(local_file_path):
                    self.convert_csv_to_parquet_and_upload(
                        local_file_path, s3_file_path
                    )
                else:
                    logger.warning(f"File not found: {local_file_path}")

            logger.info("Ingestion process completed successfully.")

        except Exception as e:
            logger.error(f"❌ Error during file ingestion: {e}")
            raise

    def run(self):
        """
        Main method to run the ingestion process.

        :raises IngestError: If the entire ingestion process fails
        """
        pipeline_start_time = time.time()

        # Start execution tracking
        self.tracker.start_pipeline_run(
            pipeline_name="ingest",
            environment=TARGET,
            username=USERNAME,
            config={"s3_upload_enabled": ENABLE_S3_UPLOAD}
        )

        try:
            logger.info(f"Starting ingestion process with TARGET={TARGET}")

            # Setup S3 secret if enabled
            if ENABLE_S3_UPLOAD:
                self.setup_s3_secret()

            # Convert and upload files
            self.convert_and_upload_files()

            # Calculate metrics
            duration = time.time() - pipeline_start_time

            # Log pipeline success metrics
            metrics.log_pipeline_run(
                status="success",
                duration=duration,
                rows_processed=self.total_rows_processed,
                pipeline_name="ingest"
            )

            # Update execution tracker
            self.tracker.end_pipeline_run(
                status="success",
                rows_processed=self.total_rows_processed,
                models_executed=self.files_processed
            )

            logger.info(
                f"Ingestion completed successfully: "
                f"{self.files_processed} files, "
                f"{self.total_rows_processed} rows in {duration:.2f}s"
            )

        except Exception as e:
            error_msg = f"Ingestion process failed: {e}"
            duration = time.time() - pipeline_start_time

            # Log failure metrics
            metrics.log_pipeline_run(
                status="failure",
                duration=duration,
                rows_processed=self.total_rows_processed,
                pipeline_name="ingest",
                error_message=str(e)
            )

            # Update execution tracker
            self.tracker.end_pipeline_run(
                status="failure",
                rows_processed=self.total_rows_processed,
                models_executed=self.files_processed,
                error_message=str(e)
            )

            # Send alert
            alert_manager.send_pipeline_failure_alert(
                pipeline_name="ingest",
                error_message=str(e),
                duration=duration,
                context={
                    "files_processed": self.files_processed,
                    "rows_processed": self.total_rows_processed
                }
            )

            log_exception(logger, e, {"context": "Ingest process"})
            raise IngestError(error_msg)
        finally:
            self.tracker.disconnect()


if __name__ == "__main__":
    try:
        ingest_process = Ingest()
        ingest_process.run()
    except Exception as e:
        logger.error(f"Ingestion process failed: {e}")
        exit(1)
