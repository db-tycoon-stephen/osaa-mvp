#!/usr/bin/env python3
"""Setup script for OSAA pipeline monitoring infrastructure.

This script initializes:
- CloudWatch alarms
- CloudWatch dashboards
- SNS topics for alerting
- Metric filters
"""

import argparse
import json
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.logging_config import create_logger
from pipeline.alerting import AlertManager

logger = create_logger(__name__)


class MonitoringSetup:
    """Setup monitoring infrastructure for OSAA pipeline."""

    def __init__(self, environment: str = "dev", region: str = "us-east-1"):
        """Initialize monitoring setup.

        Args:
            environment: Environment name (dev, qa, prod)
            region: AWS region
        """
        self.environment = environment
        self.region = region
        self.namespace = "OSAA/DataPipeline"

        # Initialize AWS clients
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
        self.sns = boto3.client('sns', region_name=region)

        logger.info(f"Initialized monitoring setup for {environment} in {region}")

    def create_sns_topic(self, topic_name: str = None) -> str:
        """Create SNS topic for alerts.

        Args:
            topic_name: Name of SNS topic

        Returns:
            Topic ARN
        """
        if not topic_name:
            topic_name = f"osaa-pipeline-alerts-{self.environment}"

        try:
            response = self.sns.create_topic(Name=topic_name)
            topic_arn = response['TopicArn']
            logger.info(f"Created SNS topic: {topic_arn}")
            return topic_arn

        except ClientError as e:
            if e.response['Error']['Code'] == 'TopicAlreadyExists':
                # Get existing topic ARN
                response = self.sns.create_topic(Name=topic_name)
                topic_arn = response['TopicArn']
                logger.info(f"Using existing SNS topic: {topic_arn}")
                return topic_arn
            else:
                logger.error(f"Failed to create SNS topic: {e}")
                raise

    def subscribe_email_to_topic(self, topic_arn: str, email: str):
        """Subscribe email address to SNS topic.

        Args:
            topic_arn: SNS topic ARN
            email: Email address to subscribe
        """
        try:
            response = self.sns.subscribe(
                TopicArn=topic_arn,
                Protocol='email',
                Endpoint=email
            )
            logger.info(f"Subscribed {email} to topic (confirmation required)")

        except ClientError as e:
            logger.error(f"Failed to subscribe email: {e}")
            raise

    def create_pipeline_failure_alarm(self, topic_arn: str):
        """Create alarm for pipeline failures.

        Args:
            topic_arn: SNS topic ARN for alarm actions
        """
        alarm_name = f"osaa-pipeline-failures-{self.environment}"

        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='PipelineFailure',
                Namespace=self.namespace,
                Period=300,
                Statistic='Sum',
                Threshold=0,
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                AlarmDescription='Alert when pipeline fails',
                Dimensions=[
                    {'Name': 'Environment', 'Value': self.environment}
                ]
            )
            logger.info(f"Created alarm: {alarm_name}")

        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise

    def create_data_freshness_alarm(self, topic_arn: str, threshold_hours: float = 26):
        """Create alarm for stale data.

        Args:
            topic_arn: SNS topic ARN for alarm actions
            threshold_hours: Alert threshold in hours
        """
        alarm_name = f"osaa-data-freshness-{self.environment}"

        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='DataFreshnessHours',
                Namespace=self.namespace,
                Period=3600,
                Statistic='Average',
                Threshold=threshold_hours,
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                AlarmDescription=f'Alert when data is older than {threshold_hours} hours',
                Dimensions=[
                    {'Name': 'Environment', 'Value': self.environment}
                ]
            )
            logger.info(f"Created alarm: {alarm_name}")

        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise

    def create_quality_score_alarm(self, topic_arn: str, threshold: float = 80):
        """Create alarm for low quality scores.

        Args:
            topic_arn: SNS topic ARN for alarm actions
            threshold: Minimum quality score
        """
        alarm_name = f"osaa-data-quality-{self.environment}"

        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='LessThanThreshold',
                EvaluationPeriods=1,
                MetricName='DataQualityScore',
                Namespace=self.namespace,
                Period=300,
                Statistic='Average',
                Threshold=threshold,
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                AlarmDescription=f'Alert when data quality score drops below {threshold}',
                Dimensions=[
                    {'Name': 'Environment', 'Value': self.environment}
                ]
            )
            logger.info(f"Created alarm: {alarm_name}")

        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise

    def create_error_rate_alarm(self, topic_arn: str, threshold: int = 5):
        """Create alarm for high error rates.

        Args:
            topic_arn: SNS topic ARN for alarm actions
            threshold: Maximum errors per period
        """
        alarm_name = f"osaa-error-rate-{self.environment}"

        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='PipelineError',
                Namespace=self.namespace,
                Period=300,
                Statistic='Sum',
                Threshold=threshold,
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                AlarmDescription=f'Alert when error rate exceeds {threshold} per 5 minutes',
                Dimensions=[
                    {'Name': 'Environment', 'Value': self.environment}
                ]
            )
            logger.info(f"Created alarm: {alarm_name}")

        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise

    def create_dashboard(self, dashboard_file: str = None):
        """Create CloudWatch dashboard from JSON file.

        Args:
            dashboard_file: Path to dashboard JSON file
        """
        if not dashboard_file:
            # Use default dashboard file
            dashboard_file = Path(__file__).parent.parent / "monitoring" / "cloudwatch_dashboard.json"

        dashboard_name = f"osaa-pipeline-{self.environment}"

        try:
            with open(dashboard_file, 'r') as f:
                dashboard_body = f.read()

            # Update dashboard body with environment
            dashboard_json = json.loads(dashboard_body)

            # Add environment dimension to all metrics
            for widget in dashboard_json.get('widgets', []):
                if widget.get('type') == 'metric':
                    properties = widget.get('properties', {})
                    for metric in properties.get('metrics', []):
                        if len(metric) > 2 and isinstance(metric[2], dict):
                            # Add environment dimension
                            if 'dimensions' not in metric[2]:
                                metric[2]['dimensions'] = {}
                            metric[2]['dimensions']['Environment'] = self.environment

            # Create dashboard
            self.cloudwatch.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=json.dumps(dashboard_json)
            )

            logger.info(f"Created dashboard: {dashboard_name}")

            # Print dashboard URL
            console_url = (
                f"https://console.aws.amazon.com/cloudwatch/home?"
                f"region={self.region}#dashboards:name={dashboard_name}"
            )
            logger.info(f"Dashboard URL: {console_url}")

        except FileNotFoundError:
            logger.error(f"Dashboard file not found: {dashboard_file}")
            raise
        except ClientError as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise

    def setup_all(self, alert_email: str = None):
        """Setup complete monitoring infrastructure.

        Args:
            alert_email: Email address for alerts (optional)
        """
        logger.info("Setting up monitoring infrastructure...")

        # Create SNS topic
        topic_arn = self.create_sns_topic()

        # Subscribe email if provided
        if alert_email:
            self.subscribe_email_to_topic(topic_arn, alert_email)

        # Create alarms
        self.create_pipeline_failure_alarm(topic_arn)
        self.create_data_freshness_alarm(topic_arn)
        self.create_quality_score_alarm(topic_arn)
        self.create_error_rate_alarm(topic_arn)

        # Create dashboard
        self.create_dashboard()

        logger.info("Monitoring setup completed successfully!")
        logger.info(f"SNS Topic ARN: {topic_arn}")
        logger.info("Add this to your .env file:")
        logger.info(f"SNS_TOPIC_ARN={topic_arn}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Setup OSAA pipeline monitoring infrastructure"
    )
    parser.add_argument(
        "--environment",
        default="dev",
        help="Environment name (dev, qa, prod)"
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region"
    )
    parser.add_argument(
        "--email",
        help="Email address for alerts"
    )
    parser.add_argument(
        "--dashboard-only",
        action="store_true",
        help="Only create dashboard (skip alarms)"
    )

    args = parser.parse_args()

    setup = MonitoringSetup(
        environment=args.environment,
        region=args.region
    )

    try:
        if args.dashboard_only:
            setup.create_dashboard()
        else:
            setup.setup_all(alert_email=args.email)

    except Exception as e:
        logger.error(f"Monitoring setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
