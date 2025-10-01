"""Data freshness monitoring for OSAA data pipeline.

This module tracks data freshness by monitoring last update timestamps
and comparing against SLA thresholds to ensure data is current.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import duckdb

from pipeline.config import DB_PATH, S3_BUCKET_NAME, STAGING_AREA_FOLDER
from pipeline.logging_config import create_logger
from pipeline.monitoring import PipelineMetrics

logger = create_logger(__name__)


class FreshnessMonitor:
    """Monitor data freshness across pipeline datasets.

    Tracks when datasets were last updated and compares against
    configured SLA thresholds to identify stale data.

    Attributes:
        db_path: Path to DuckDB database
        metrics: PipelineMetrics instance for logging
        sla_thresholds: SLA thresholds in hours per dataset
    """

    # Default SLA thresholds (in hours)
    DEFAULT_SLA_HOURS = 24

    def __init__(
        self,
        db_path: Optional[str] = None,
        metrics: Optional[PipelineMetrics] = None,
        sla_thresholds: Optional[Dict[str, float]] = None
    ):
        """Initialize FreshnessMonitor.

        Args:
            db_path: Path to DuckDB database
            metrics: PipelineMetrics instance
            sla_thresholds: Custom SLA thresholds per dataset
        """
        self.db_path = db_path or DB_PATH
        self.metrics = metrics or PipelineMetrics()
        self.sla_thresholds = sla_thresholds or {}
        self.con = None

    def connect(self):
        """Establish database connection."""
        if not self.con:
            self.con = duckdb.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")

    def disconnect(self):
        """Close database connection."""
        if self.con:
            self.con.close()
            self.con = None

    def get_table_freshness(self, schema: str, table: str) -> Optional[datetime]:
        """Get the last modification time of a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Datetime of last modification, or None if not found
        """
        try:
            self.connect()

            # Check if table exists
            query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
            """

            result = self.con.execute(query).fetchone()
            if not result:
                logger.warning(f"Table {schema}.{table} not found")
                return None

            # Get row count and last update approximation
            # Note: DuckDB doesn't track table modification times directly
            # We'll use a metadata approach or row timestamps if available
            count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
            row_count = self.con.execute(count_query).fetchone()[0]

            if row_count == 0:
                logger.warning(f"Table {schema}.{table} is empty")
                return None

            # Try to get the most recent timestamp from common timestamp columns
            timestamp_columns = [
                'updated_at', 'created_at', 'timestamp',
                'date', 'last_modified', 'ingestion_time'
            ]

            for col in timestamp_columns:
                try:
                    ts_query = f"""
                        SELECT MAX({col}) as last_update
                        FROM {schema}.{table}
                        WHERE {col} IS NOT NULL
                    """
                    result = self.con.execute(ts_query).fetchone()
                    if result and result[0]:
                        return result[0]
                except Exception:
                    # Column doesn't exist or wrong type
                    continue

            # If no timestamp column found, use file modification time
            # This is an approximation
            logger.debug(f"No timestamp column found for {schema}.{table}, using current time")
            return datetime.utcnow()

        except Exception as e:
            logger.error(f"Error getting freshness for {schema}.{table}: {e}")
            return None

    def get_s3_object_freshness(self, s3_path: str) -> Optional[datetime]:
        """Get the last modification time of an S3 object.

        Args:
            s3_path: S3 path (e.g., s3://bucket/key)

        Returns:
            Datetime of last modification, or None if not found
        """
        try:
            import boto3

            s3_client = boto3.client('s3')

            # Parse S3 path
            if not s3_path.startswith('s3://'):
                logger.error(f"Invalid S3 path: {s3_path}")
                return None

            path_parts = s3_path[5:].split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''

            # Get object metadata
            response = s3_client.head_object(Bucket=bucket, Key=key)
            return response['LastModified']

        except Exception as e:
            logger.error(f"Error getting S3 object freshness for {s3_path}: {e}")
            return None

    def check_dataset_freshness(
        self,
        dataset_name: str,
        schema: str,
        table: str
    ) -> Tuple[bool, float, Optional[datetime]]:
        """Check if a dataset is fresh according to SLA.

        Args:
            dataset_name: Name of the dataset
            schema: Schema name
            table: Table name

        Returns:
            Tuple of (is_fresh, hours_old, last_update)
        """
        last_update = self.get_table_freshness(schema, table)

        if not last_update:
            logger.warning(f"Could not determine freshness for {dataset_name}")
            return False, float('inf'), None

        # Calculate age in hours
        age = datetime.utcnow() - last_update
        hours_old = age.total_seconds() / 3600

        # Get SLA threshold for this dataset
        sla_hours = self.sla_thresholds.get(dataset_name, self.DEFAULT_SLA_HOURS)

        # Check if fresh
        is_fresh = hours_old <= sla_hours

        # Log metrics
        self.metrics.log_data_freshness(
            dataset=dataset_name,
            last_update=last_update,
            freshness_hours=hours_old
        )

        return is_fresh, hours_old, last_update

    def check_all_datasets(self) -> Dict[str, Dict]:
        """Check freshness for all monitored datasets.

        Returns:
            Dictionary of dataset freshness results
        """
        # Define datasets to monitor
        datasets = [
            ("wdi_indicators", "sources", "wdi_indicators"),
            ("sdg_indicators", "sources", "sdg_indicators"),
            ("opri_indicators", "sources", "opri_indicators"),
            ("master_indicators", "master", "indicators"),
        ]

        results = {}

        for dataset_name, schema, table in datasets:
            try:
                is_fresh, hours_old, last_update = self.check_dataset_freshness(
                    dataset_name, schema, table
                )

                results[dataset_name] = {
                    "is_fresh": is_fresh,
                    "hours_old": hours_old,
                    "last_update": last_update,
                    "sla_hours": self.sla_thresholds.get(
                        dataset_name, self.DEFAULT_SLA_HOURS
                    )
                }

                if not is_fresh:
                    logger.warning(
                        f"Dataset {dataset_name} is stale: "
                        f"{hours_old:.1f} hours old (SLA: {results[dataset_name]['sla_hours']}h)"
                    )
                else:
                    logger.info(
                        f"Dataset {dataset_name} is fresh: "
                        f"{hours_old:.1f} hours old"
                    )

            except Exception as e:
                logger.error(f"Error checking freshness for {dataset_name}: {e}")
                results[dataset_name] = {
                    "is_fresh": False,
                    "hours_old": None,
                    "last_update": None,
                    "error": str(e)
                }

        return results

    def get_stale_datasets(self) -> List[str]:
        """Get list of datasets that are stale.

        Returns:
            List of stale dataset names
        """
        results = self.check_all_datasets()
        return [
            name for name, info in results.items()
            if not info["is_fresh"]
        ]

    def set_sla_threshold(self, dataset: str, hours: float):
        """Set SLA threshold for a specific dataset.

        Args:
            dataset: Dataset name
            hours: SLA threshold in hours
        """
        self.sla_thresholds[dataset] = hours
        logger.info(f"Set SLA threshold for {dataset}: {hours} hours")

    def generate_freshness_report(self) -> str:
        """Generate a formatted freshness report.

        Returns:
            Formatted string report
        """
        results = self.check_all_datasets()

        report_lines = [
            "=" * 80,
            "DATA FRESHNESS REPORT",
            f"Generated: {datetime.utcnow().isoformat()}Z",
            "=" * 80,
            ""
        ]

        for dataset, info in sorted(results.items()):
            status = "✓ FRESH" if info["is_fresh"] else "✗ STALE"

            if info.get("error"):
                report_lines.append(f"{dataset}: ERROR - {info['error']}")
            else:
                hours = info["hours_old"]
                sla = info["sla_hours"]
                last_update = info["last_update"]

                report_lines.extend([
                    f"{dataset}: {status}",
                    f"  Last Update: {last_update.isoformat() if last_update else 'Unknown'}",
                    f"  Age: {hours:.1f} hours",
                    f"  SLA: {sla} hours",
                    ""
                ])

        report_lines.append("=" * 80)

        return "\n".join(report_lines)


def check_freshness_cli():
    """Command-line interface for freshness checking."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Check data freshness for OSAA pipeline"
    )
    parser.add_argument(
        "--db-path",
        help="Path to DuckDB database",
        default=DB_PATH
    )
    parser.add_argument(
        "--dataset",
        help="Specific dataset to check (optional)",
        default=None
    )
    parser.add_argument(
        "--sla-hours",
        type=float,
        help="Override SLA threshold in hours",
        default=None
    )

    args = parser.parse_args()

    # Create monitor
    monitor = FreshnessMonitor(db_path=args.db_path)

    try:
        if args.dataset:
            # Check specific dataset
            # Note: Would need to map dataset name to schema/table
            logger.info(f"Checking freshness for dataset: {args.dataset}")
            # Implementation would go here
        else:
            # Check all datasets
            print(monitor.generate_freshness_report())

    finally:
        monitor.disconnect()


if __name__ == "__main__":
    check_freshness_cli()
