"""CloudWatch monitoring and metrics collection for OSAA data pipeline.

This module provides comprehensive monitoring capabilities including:
- Pipeline execution metrics (success/failure, duration, row counts)
- Data freshness tracking
- Model execution metrics
- Quality score tracking
- Error tracking and alerting
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional, Union

import boto3
from botocore.exceptions import ClientError

from pipeline.logging_config import create_logger

logger = create_logger(__name__)


class PipelineMetrics:
    """Manages CloudWatch metrics for the OSAA data pipeline.

    This class provides methods to log various pipeline metrics to AWS CloudWatch,
    enabling comprehensive monitoring and alerting capabilities.

    Attributes:
        namespace: CloudWatch namespace for all metrics
        environment: Current environment (dev, qa, prod)
        cloudwatch_client: Boto3 CloudWatch client
    """

    def __init__(
        self,
        namespace: str = "OSAA/DataPipeline",
        environment: Optional[str] = None,
        region_name: str = "us-east-1"
    ):
        """Initialize PipelineMetrics with CloudWatch client.

        Args:
            namespace: CloudWatch namespace for metrics
            environment: Environment name (dev, qa, prod)
            region_name: AWS region for CloudWatch
        """
        self.namespace = namespace
        self.environment = environment or "dev"

        try:
            self.cloudwatch_client = boto3.client('cloudwatch', region_name=region_name)
            logger.info(f"Initialized CloudWatch metrics with namespace: {namespace}")
        except Exception as e:
            logger.error(f"Failed to initialize CloudWatch client: {e}")
            self.cloudwatch_client = None

    def _publish_metric(
        self,
        metric_name: str,
        value: Union[int, float],
        unit: str = "None",
        dimensions: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Publish a metric to CloudWatch.

        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: CloudWatch unit type
            dimensions: Additional dimensions for the metric
            timestamp: Timestamp for the metric (defaults to now)

        Returns:
            True if successful, False otherwise
        """
        if not self.cloudwatch_client:
            logger.warning(f"CloudWatch client not available, skipping metric: {metric_name}")
            return False

        try:
            # Build dimensions with environment
            metric_dimensions = [
                {"Name": "Environment", "Value": self.environment}
            ]

            if dimensions:
                for key, value in dimensions.items():
                    metric_dimensions.append({"Name": key, "Value": str(value)})

            # Build metric data
            metric_data = {
                "MetricName": metric_name,
                "Value": float(value),
                "Unit": unit,
                "Timestamp": timestamp or datetime.utcnow(),
                "Dimensions": metric_dimensions
            }

            # Publish to CloudWatch
            self.cloudwatch_client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )

            logger.debug(f"Published metric: {metric_name} = {value} {unit}")
            return True

        except ClientError as e:
            logger.error(f"Failed to publish metric {metric_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing metric {metric_name}: {e}")
            return False

    def log_pipeline_run(
        self,
        status: str,
        duration: float,
        rows_processed: int,
        pipeline_name: str = "main",
        error_message: Optional[str] = None
    ) -> None:
        """Log pipeline execution metrics.

        Args:
            status: Pipeline run status (success, failure, partial)
            duration: Duration in seconds
            rows_processed: Number of rows processed
            pipeline_name: Name of the pipeline
            error_message: Error message if failed
        """
        dimensions = {"PipelineName": pipeline_name}

        # Log success/failure
        success_value = 1 if status == "success" else 0
        self._publish_metric(
            "PipelineSuccess",
            success_value,
            unit="Count",
            dimensions=dimensions
        )

        # Log duration
        self._publish_metric(
            "PipelineDuration",
            duration,
            unit="Seconds",
            dimensions=dimensions
        )

        # Log rows processed
        self._publish_metric(
            "RowsProcessed",
            rows_processed,
            unit="Count",
            dimensions=dimensions
        )

        # Log failure with error context
        if status == "failure" and error_message:
            logger.error(f"Pipeline {pipeline_name} failed: {error_message}")
            self._publish_metric(
                "PipelineFailure",
                1,
                unit="Count",
                dimensions={**dimensions, "ErrorType": error_message[:50]}
            )

    def log_model_execution(
        self,
        model_name: str,
        duration: float,
        row_count: int,
        status: str = "success"
    ) -> None:
        """Log individual model execution metrics.

        Args:
            model_name: Name of the SQLMesh model
            duration: Execution duration in seconds
            row_count: Number of rows in model output
            status: Execution status
        """
        dimensions = {"ModelName": model_name}

        # Log execution time
        self._publish_metric(
            "ModelExecutionTime",
            duration,
            unit="Seconds",
            dimensions=dimensions
        )

        # Log row count
        self._publish_metric(
            "ModelRowCount",
            row_count,
            unit="Count",
            dimensions=dimensions
        )

        # Log success/failure
        success_value = 1 if status == "success" else 0
        self._publish_metric(
            "ModelSuccess",
            success_value,
            unit="Count",
            dimensions=dimensions
        )

    def log_data_freshness(
        self,
        dataset: str,
        last_update: datetime,
        freshness_hours: float
    ) -> None:
        """Log data freshness metrics.

        Args:
            dataset: Name of the dataset
            last_update: Timestamp of last update
            freshness_hours: Hours since last update
        """
        dimensions = {"Dataset": dataset}

        # Log freshness in hours
        self._publish_metric(
            "DataFreshnessHours",
            freshness_hours,
            unit="None",
            dimensions=dimensions
        )

        # Log last update timestamp as a metric value
        last_update_epoch = last_update.timestamp()
        self._publish_metric(
            "LastUpdateTimestamp",
            last_update_epoch,
            unit="None",
            dimensions=dimensions
        )

    def log_quality_score(
        self,
        dataset: str,
        score: float,
        check_type: str = "overall"
    ) -> None:
        """Log data quality scores.

        Args:
            dataset: Name of the dataset
            score: Quality score (0-100)
            check_type: Type of quality check
        """
        dimensions = {
            "Dataset": dataset,
            "CheckType": check_type
        }

        self._publish_metric(
            "DataQualityScore",
            score,
            unit="None",
            dimensions=dimensions
        )

    def log_error(
        self,
        error_type: str,
        error_message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log error metrics with context.

        Args:
            error_type: Type/category of error
            error_message: Error message
            context: Additional context information
        """
        dimensions = {"ErrorType": error_type}

        if context:
            # Add context as dimensions (CloudWatch limit: 10 dimensions)
            for key, value in list(context.items())[:8]:  # Leave room for ErrorType and Environment
                dimensions[key] = str(value)

        # Log error occurrence
        self._publish_metric(
            "PipelineError",
            1,
            unit="Count",
            dimensions=dimensions
        )

        # Log detailed error
        logger.error(f"Pipeline error [{error_type}]: {error_message}")
        if context:
            logger.error(f"Error context: {context}")

    def log_s3_upload(
        self,
        file_path: str,
        file_size_mb: float,
        upload_duration: float,
        status: str = "success"
    ) -> None:
        """Log S3 upload metrics.

        Args:
            file_path: S3 file path
            file_size_mb: File size in MB
            upload_duration: Upload duration in seconds
            status: Upload status
        """
        dimensions = {"Status": status}

        # Log file size
        self._publish_metric(
            "S3UploadSizeMB",
            file_size_mb,
            unit="None",
            dimensions=dimensions
        )

        # Log upload duration
        self._publish_metric(
            "S3UploadDuration",
            upload_duration,
            unit="Seconds",
            dimensions=dimensions
        )

        # Calculate and log throughput
        if upload_duration > 0:
            throughput = file_size_mb / upload_duration
            self._publish_metric(
                "S3UploadThroughput",
                throughput,
                unit="None",
                dimensions=dimensions
            )

    def log_data_volume(
        self,
        source: str,
        volume_mb: float,
        record_count: int
    ) -> None:
        """Log data volume metrics by source.

        Args:
            source: Data source name
            volume_mb: Data volume in MB
            record_count: Number of records
        """
        dimensions = {"Source": source}

        self._publish_metric(
            "DataVolumeMB",
            volume_mb,
            unit="None",
            dimensions=dimensions
        )

        self._publish_metric(
            "DataRecordCount",
            record_count,
            unit="Count",
            dimensions=dimensions
        )


class MetricsContext:
    """Context manager for tracking operation duration and metrics.

    Usage:
        with MetricsContext(metrics, "operation_name") as ctx:
            # Do work
            ctx.set_row_count(1000)
    """

    def __init__(
        self,
        metrics: PipelineMetrics,
        operation_name: str,
        dimensions: Optional[Dict[str, str]] = None
    ):
        """Initialize metrics context.

        Args:
            metrics: PipelineMetrics instance
            operation_name: Name of the operation
            dimensions: Additional dimensions
        """
        self.metrics = metrics
        self.operation_name = operation_name
        self.dimensions = dimensions or {}
        self.start_time = None
        self.row_count = 0
        self.error = None

    def __enter__(self):
        """Start timing the operation."""
        self.start_time = time.time()
        logger.info(f"Starting {self.operation_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Log metrics when operation completes."""
        duration = time.time() - self.start_time

        if exc_type is None:
            # Success case
            logger.info(
                f"Completed {self.operation_name} in {duration:.2f}s "
                f"({self.row_count} rows)"
            )
            self.metrics._publish_metric(
                f"{self.operation_name}Duration",
                duration,
                unit="Seconds",
                dimensions=self.dimensions
            )
        else:
            # Error case
            logger.error(
                f"Failed {self.operation_name} after {duration:.2f}s: {exc_val}"
            )
            self.metrics.log_error(
                error_type=exc_type.__name__,
                error_message=str(exc_val),
                context={"operation": self.operation_name, **self.dimensions}
            )

        return False  # Don't suppress exceptions

    def set_row_count(self, count: int):
        """Set the row count for this operation."""
        self.row_count = count


# Singleton instance for easy access
_metrics_instance: Optional[PipelineMetrics] = None


def get_metrics(
    namespace: str = "OSAA/DataPipeline",
    environment: Optional[str] = None
) -> PipelineMetrics:
    """Get or create the global metrics instance.

    Args:
        namespace: CloudWatch namespace
        environment: Environment name

    Returns:
        PipelineMetrics instance
    """
    global _metrics_instance

    if _metrics_instance is None:
        _metrics_instance = PipelineMetrics(namespace, environment)

    return _metrics_instance
