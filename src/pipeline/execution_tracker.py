"""Execution tracking for OSAA data pipeline.

This module tracks detailed execution metadata including:
- Pipeline run information
- Model-level execution times
- Data lineage
- Version tracking
- Historical analysis storage
"""

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb

from pipeline.config import DB_PATH
from pipeline.logging_config import create_logger

logger = create_logger(__name__)


class ExecutionTracker:
    """Track detailed execution metadata for pipeline runs.

    Stores comprehensive execution information in DuckDB for:
    - Debugging and troubleshooting
    - Performance analysis
    - Data lineage tracking
    - Audit and compliance

    Attributes:
        db_path: Path to DuckDB database
        con: DuckDB connection
        run_id: Unique identifier for current run
    """

    def __init__(self, db_path: Optional[str] = None):
        """Initialize ExecutionTracker.

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = db_path or DB_PATH
        self.con = None
        self.run_id = str(uuid.uuid4())
        self.run_start_time = datetime.utcnow()

        # Initialize tracking tables
        self._init_tracking_tables()

    def connect(self):
        """Establish database connection."""
        if not self.con:
            self.con = duckdb.connect(self.db_path)
            logger.debug(f"Connected to database: {self.db_path}")

    def disconnect(self):
        """Close database connection."""
        if self.con:
            self.con.close()
            self.con = None

    def _init_tracking_tables(self):
        """Initialize execution tracking tables if they don't exist."""
        self.connect()

        try:
            # Create schema for tracking
            self.con.execute("CREATE SCHEMA IF NOT EXISTS _tracking")

            # Pipeline runs table
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS _tracking.pipeline_runs (
                    run_id VARCHAR PRIMARY KEY,
                    pipeline_name VARCHAR,
                    environment VARCHAR,
                    username VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_seconds DOUBLE,
                    status VARCHAR,
                    error_message TEXT,
                    rows_processed BIGINT,
                    models_executed INTEGER,
                    config JSON,
                    git_commit VARCHAR,
                    git_branch VARCHAR
                )
            """)

            # Model executions table
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS _tracking.model_executions (
                    execution_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR,
                    model_name VARCHAR,
                    model_type VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_seconds DOUBLE,
                    status VARCHAR,
                    rows_produced BIGINT,
                    bytes_written BIGINT,
                    error_message TEXT,
                    dependencies JSON,
                    metadata JSON,
                    FOREIGN KEY (run_id) REFERENCES _tracking.pipeline_runs(run_id)
                )
            """)

            # Data lineage table
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS _tracking.data_lineage (
                    lineage_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR,
                    source_table VARCHAR,
                    target_table VARCHAR,
                    transformation_type VARCHAR,
                    row_count BIGINT,
                    timestamp TIMESTAMP,
                    metadata JSON,
                    FOREIGN KEY (run_id) REFERENCES _tracking.pipeline_runs(run_id)
                )
            """)

            # Metrics table for custom metrics
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS _tracking.custom_metrics (
                    metric_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR,
                    metric_name VARCHAR,
                    metric_value DOUBLE,
                    metric_unit VARCHAR,
                    dimensions JSON,
                    timestamp TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES _tracking.pipeline_runs(run_id)
                )
            """)

            logger.info("Initialized execution tracking tables")

        except Exception as e:
            logger.error(f"Failed to initialize tracking tables: {e}")
            raise

    def start_pipeline_run(
        self,
        pipeline_name: str,
        environment: Optional[str] = None,
        username: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Start tracking a new pipeline run.

        Args:
            pipeline_name: Name of the pipeline
            environment: Environment (dev/qa/prod)
            username: User running the pipeline
            config: Pipeline configuration

        Returns:
            Run ID
        """
        self.connect()

        # Get git information if available
        git_commit, git_branch = self._get_git_info()

        try:
            self.con.execute("""
                INSERT INTO _tracking.pipeline_runs (
                    run_id, pipeline_name, environment, username,
                    start_time, status, config, git_commit, git_branch
                )
                VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?)
            """, [
                self.run_id,
                pipeline_name,
                environment or os.getenv("TARGET", "dev"),
                username or os.getenv("USERNAME", "unknown"),
                self.run_start_time,
                json.dumps(config) if config else None,
                git_commit,
                git_branch
            ])

            logger.info(f"Started tracking pipeline run: {self.run_id}")
            return self.run_id

        except Exception as e:
            logger.error(f"Failed to start pipeline run tracking: {e}")
            raise

    def end_pipeline_run(
        self,
        status: str,
        rows_processed: int = 0,
        models_executed: int = 0,
        error_message: Optional[str] = None
    ):
        """End tracking for current pipeline run.

        Args:
            status: Final status (success/failure/partial)
            rows_processed: Total rows processed
            models_executed: Number of models executed
            error_message: Error message if failed
        """
        self.connect()

        end_time = datetime.utcnow()
        duration = (end_time - self.run_start_time).total_seconds()

        try:
            self.con.execute("""
                UPDATE _tracking.pipeline_runs
                SET end_time = ?,
                    duration_seconds = ?,
                    status = ?,
                    rows_processed = ?,
                    models_executed = ?,
                    error_message = ?
                WHERE run_id = ?
            """, [
                end_time,
                duration,
                status,
                rows_processed,
                models_executed,
                error_message,
                self.run_id
            ])

            logger.info(
                f"Completed pipeline run {self.run_id}: "
                f"{status} in {duration:.2f}s"
            )

        except Exception as e:
            logger.error(f"Failed to end pipeline run tracking: {e}")

    def track_model_execution(
        self,
        model_name: str,
        model_type: str,
        start_time: datetime,
        end_time: datetime,
        status: str,
        rows_produced: int = 0,
        bytes_written: int = 0,
        dependencies: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> str:
        """Track execution of a single model.

        Args:
            model_name: Name of the model
            model_type: Type of model (FULL, INCREMENTAL, etc.)
            start_time: Start timestamp
            end_time: End timestamp
            status: Execution status
            rows_produced: Number of rows produced
            bytes_written: Bytes written
            dependencies: List of dependent models
            metadata: Additional metadata
            error_message: Error message if failed

        Returns:
            Execution ID
        """
        self.connect()

        execution_id = str(uuid.uuid4())
        duration = (end_time - start_time).total_seconds()

        try:
            self.con.execute("""
                INSERT INTO _tracking.model_executions (
                    execution_id, run_id, model_name, model_type,
                    start_time, end_time, duration_seconds, status,
                    rows_produced, bytes_written, error_message,
                    dependencies, metadata
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                execution_id,
                self.run_id,
                model_name,
                model_type,
                start_time,
                end_time,
                duration,
                status,
                rows_produced,
                bytes_written,
                error_message,
                json.dumps(dependencies) if dependencies else None,
                json.dumps(metadata) if metadata else None
            ])

            logger.debug(
                f"Tracked model execution: {model_name} "
                f"({duration:.2f}s, {rows_produced} rows)"
            )

            return execution_id

        except Exception as e:
            logger.error(f"Failed to track model execution: {e}")
            return ""

    def track_lineage(
        self,
        source_table: str,
        target_table: str,
        transformation_type: str,
        row_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Track data lineage between tables.

        Args:
            source_table: Source table name
            target_table: Target table name
            transformation_type: Type of transformation
            row_count: Number of rows in transformation
            metadata: Additional metadata

        Returns:
            Lineage ID
        """
        self.connect()

        lineage_id = str(uuid.uuid4())
        timestamp = datetime.utcnow()

        try:
            self.con.execute("""
                INSERT INTO _tracking.data_lineage (
                    lineage_id, run_id, source_table, target_table,
                    transformation_type, row_count, timestamp, metadata
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                lineage_id,
                self.run_id,
                source_table,
                target_table,
                transformation_type,
                row_count,
                timestamp,
                json.dumps(metadata) if metadata else None
            ])

            logger.debug(
                f"Tracked lineage: {source_table} -> {target_table} "
                f"({transformation_type})"
            )

            return lineage_id

        except Exception as e:
            logger.error(f"Failed to track lineage: {e}")
            return ""

    def track_custom_metric(
        self,
        metric_name: str,
        metric_value: float,
        metric_unit: str = "None",
        dimensions: Optional[Dict[str, str]] = None
    ) -> str:
        """Track custom metric.

        Args:
            metric_name: Name of the metric
            metric_value: Metric value
            metric_unit: Unit of measurement
            dimensions: Additional dimensions

        Returns:
            Metric ID
        """
        self.connect()

        metric_id = str(uuid.uuid4())
        timestamp = datetime.utcnow()

        try:
            self.con.execute("""
                INSERT INTO _tracking.custom_metrics (
                    metric_id, run_id, metric_name, metric_value,
                    metric_unit, dimensions, timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                metric_id,
                self.run_id,
                metric_name,
                metric_value,
                metric_unit,
                json.dumps(dimensions) if dimensions else None,
                timestamp
            ])

            logger.debug(f"Tracked custom metric: {metric_name} = {metric_value}")

            return metric_id

        except Exception as e:
            logger.error(f"Failed to track custom metric: {e}")
            return ""

    def get_run_summary(self, run_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get summary for a pipeline run.

        Args:
            run_id: Run ID (defaults to current run)

        Returns:
            Dictionary with run summary
        """
        self.connect()

        run_id = run_id or self.run_id

        try:
            result = self.con.execute("""
                SELECT
                    run_id,
                    pipeline_name,
                    environment,
                    username,
                    start_time,
                    end_time,
                    duration_seconds,
                    status,
                    rows_processed,
                    models_executed,
                    error_message
                FROM _tracking.pipeline_runs
                WHERE run_id = ?
            """, [run_id]).fetchone()

            if not result:
                return None

            return {
                "run_id": result[0],
                "pipeline_name": result[1],
                "environment": result[2],
                "username": result[3],
                "start_time": result[4],
                "end_time": result[5],
                "duration_seconds": result[6],
                "status": result[7],
                "rows_processed": result[8],
                "models_executed": result[9],
                "error_message": result[10]
            }

        except Exception as e:
            logger.error(f"Failed to get run summary: {e}")
            return None

    def get_recent_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent pipeline runs.

        Args:
            limit: Number of runs to retrieve

        Returns:
            List of run summaries
        """
        self.connect()

        try:
            results = self.con.execute("""
                SELECT
                    run_id,
                    pipeline_name,
                    environment,
                    start_time,
                    duration_seconds,
                    status,
                    rows_processed
                FROM _tracking.pipeline_runs
                ORDER BY start_time DESC
                LIMIT ?
            """, [limit]).fetchall()

            return [
                {
                    "run_id": row[0],
                    "pipeline_name": row[1],
                    "environment": row[2],
                    "start_time": row[3],
                    "duration_seconds": row[4],
                    "status": row[5],
                    "rows_processed": row[6]
                }
                for row in results
            ]

        except Exception as e:
            logger.error(f"Failed to get recent runs: {e}")
            return []

    def _get_git_info(self) -> tuple:
        """Get current git commit and branch.

        Returns:
            Tuple of (commit_hash, branch_name)
        """
        try:
            import subprocess

            commit = subprocess.check_output(
                ['git', 'rev-parse', 'HEAD'],
                stderr=subprocess.DEVNULL
            ).decode('utf-8').strip()

            branch = subprocess.check_output(
                ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                stderr=subprocess.DEVNULL
            ).decode('utf-8').strip()

            return commit, branch

        except Exception:
            return None, None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is None:
            # Success
            self.end_pipeline_run(status="success")
        else:
            # Failure
            self.end_pipeline_run(
                status="failure",
                error_message=str(exc_val)
            )

        self.disconnect()
        return False  # Don't suppress exceptions
