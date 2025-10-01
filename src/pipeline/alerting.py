"""Alerting system for OSAA data pipeline.

This module provides multi-channel alerting capabilities including:
- Slack notifications
- Email alerts via AWS SES
- CloudWatch Alarms
- PagerDuty integration (optional)
"""

import json
import os
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from pipeline.logging_config import create_logger

logger = create_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class AlertManager:
    """Manages alerts across multiple channels.

    Provides unified interface for sending alerts via:
    - Slack
    - Email (AWS SES)
    - CloudWatch Alarms
    - PagerDuty (optional)

    Attributes:
        slack_webhook_url: Slack webhook URL
        email_from: Source email address
        email_to: List of recipient email addresses
        ses_client: AWS SES client
        cloudwatch_client: AWS CloudWatch client
        sns_client: AWS SNS client
    """

    def __init__(
        self,
        slack_webhook_url: Optional[str] = None,
        email_from: Optional[str] = None,
        email_to: Optional[List[str]] = None,
        sns_topic_arn: Optional[str] = None,
        region_name: str = "us-east-1"
    ):
        """Initialize AlertManager.

        Args:
            slack_webhook_url: Slack webhook URL
            email_from: Source email address for SES
            email_to: List of recipient email addresses
            sns_topic_arn: SNS topic ARN for notifications
            region_name: AWS region
        """
        # Get configuration from environment if not provided
        self.slack_webhook_url = slack_webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        self.email_from = email_from or os.getenv("ALERT_EMAIL_FROM", "noreply@osaa-pipeline.com")
        self.email_to = email_to or self._parse_email_list(os.getenv("ALERT_EMAIL_TO", ""))
        self.sns_topic_arn = sns_topic_arn or os.getenv("SNS_TOPIC_ARN")

        # Initialize AWS clients
        try:
            self.ses_client = boto3.client('ses', region_name=region_name)
            self.cloudwatch_client = boto3.client('cloudwatch', region_name=region_name)
            self.sns_client = boto3.client('sns', region_name=region_name)
            logger.info("Initialized AWS clients for alerting")
        except Exception as e:
            logger.warning(f"Failed to initialize AWS clients: {e}")
            self.ses_client = None
            self.cloudwatch_client = None
            self.sns_client = None

    def _parse_email_list(self, email_str: str) -> List[str]:
        """Parse comma-separated email list."""
        if not email_str:
            return []
        return [email.strip() for email in email_str.split(",") if email.strip()]

    def send_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        context: Optional[Dict[str, Any]] = None,
        channels: Optional[List[str]] = None
    ) -> Dict[str, bool]:
        """Send alert across configured channels.

        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity level
            context: Additional context information
            channels: Specific channels to use (default: all)

        Returns:
            Dictionary of channel results (success/failure)
        """
        results = {}

        # Default to all channels if none specified
        if channels is None:
            channels = ["slack", "email", "sns"]

        # Format context
        context_str = self._format_context(context) if context else ""

        # Send to each channel
        if "slack" in channels and self.slack_webhook_url:
            results["slack"] = self._send_slack_alert(
                title, message, severity, context_str
            )

        if "email" in channels and self.email_to:
            results["email"] = self._send_email_alert(
                title, message, severity, context_str
            )

        if "sns" in channels and self.sns_topic_arn:
            results["sns"] = self._send_sns_alert(
                title, message, severity, context_str
            )

        return results

    def _format_context(self, context: Dict[str, Any]) -> str:
        """Format context dictionary as readable string."""
        lines = []
        for key, value in context.items():
            lines.append(f"{key}: {value}")
        return "\n".join(lines)

    def _send_slack_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        context: str
    ) -> bool:
        """Send alert to Slack.

        Args:
            title: Alert title
            message: Alert message
            severity: Severity level
            context: Formatted context

        Returns:
            True if successful, False otherwise
        """
        try:
            import requests

            # Determine color based on severity
            color_map = {
                AlertSeverity.CRITICAL: "#FF0000",  # Red
                AlertSeverity.HIGH: "#FF6600",      # Orange
                AlertSeverity.MEDIUM: "#FFCC00",    # Yellow
                AlertSeverity.LOW: "#3366FF",       # Blue
                AlertSeverity.INFO: "#36A64F"       # Green
            }
            color = color_map.get(severity, "#808080")

            # Build Slack message
            slack_message = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"[{severity.value}] {title}",
                        "text": message,
                        "fields": [
                            {
                                "title": "Severity",
                                "value": severity.value,
                                "short": True
                            },
                            {
                                "title": "Timestamp",
                                "value": datetime.utcnow().isoformat() + "Z",
                                "short": True
                            }
                        ],
                        "footer": "OSAA Data Pipeline",
                        "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png"
                    }
                ]
            }

            # Add context if present
            if context:
                slack_message["attachments"][0]["fields"].append({
                    "title": "Context",
                    "value": f"```{context}```",
                    "short": False
                })

            # Send to Slack
            response = requests.post(
                self.slack_webhook_url,
                json=slack_message,
                timeout=10
            )
            response.raise_for_status()

            logger.info(f"Sent Slack alert: {title}")
            return True

        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False

    def _send_email_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        context: str
    ) -> bool:
        """Send alert via email using AWS SES.

        Args:
            title: Alert title
            message: Alert message
            severity: Severity level
            context: Formatted context

        Returns:
            True if successful, False otherwise
        """
        if not self.ses_client:
            logger.warning("SES client not initialized, skipping email alert")
            return False

        try:
            # Build email body
            body_text = f"""
OSAA Data Pipeline Alert

Severity: {severity.value}
Title: {title}

Message:
{message}

{f"Context:\n{context}" if context else ""}

Timestamp: {datetime.utcnow().isoformat()}Z

---
This is an automated alert from the OSAA Data Pipeline monitoring system.
"""

            body_html = f"""
<html>
<head></head>
<body>
    <h2 style="color: #333;">OSAA Data Pipeline Alert</h2>
    <p><strong>Severity:</strong> <span style="color: {'red' if severity in [AlertSeverity.CRITICAL, AlertSeverity.HIGH] else 'orange'};">{severity.value}</span></p>
    <p><strong>Title:</strong> {title}</p>
    <h3>Message:</h3>
    <p>{message}</p>
    {f"<h3>Context:</h3><pre>{context}</pre>" if context else ""}
    <p><small>Timestamp: {datetime.utcnow().isoformat()}Z</small></p>
    <hr>
    <p><small>This is an automated alert from the OSAA Data Pipeline monitoring system.</small></p>
</body>
</html>
"""

            # Send email
            response = self.ses_client.send_email(
                Source=self.email_from,
                Destination={
                    'ToAddresses': self.email_to
                },
                Message={
                    'Subject': {
                        'Data': f"[{severity.value}] {title}",
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Text': {
                            'Data': body_text,
                            'Charset': 'UTF-8'
                        },
                        'Html': {
                            'Data': body_html,
                            'Charset': 'UTF-8'
                        }
                    }
                }
            )

            logger.info(f"Sent email alert to {len(self.email_to)} recipients: {title}")
            return True

        except ClientError as e:
            logger.error(f"Failed to send email alert: {e}")
            return False

    def _send_sns_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        context: str
    ) -> bool:
        """Send alert via AWS SNS.

        Args:
            title: Alert title
            message: Alert message
            severity: Severity level
            context: Formatted context

        Returns:
            True if successful, False otherwise
        """
        if not self.sns_client or not self.sns_topic_arn:
            logger.warning("SNS not configured, skipping SNS alert")
            return False

        try:
            # Build SNS message
            sns_message = f"""
[{severity.value}] {title}

{message}

{f"Context:\n{context}" if context else ""}

Timestamp: {datetime.utcnow().isoformat()}Z
"""

            # Publish to SNS
            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=f"[{severity.value}] {title}",
                Message=sns_message
            )

            logger.info(f"Sent SNS alert: {title}")
            return True

        except ClientError as e:
            logger.error(f"Failed to send SNS alert: {e}")
            return False

    def create_cloudwatch_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        namespace: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Average",
        alarm_description: Optional[str] = None,
        dimensions: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        """Create a CloudWatch alarm.

        Args:
            alarm_name: Name of the alarm
            metric_name: CloudWatch metric name
            namespace: CloudWatch namespace
            threshold: Alarm threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of periods to evaluate
            period: Period in seconds
            statistic: Statistic to use
            alarm_description: Optional description
            dimensions: Metric dimensions

        Returns:
            True if successful, False otherwise
        """
        if not self.cloudwatch_client:
            logger.warning("CloudWatch client not initialized")
            return False

        try:
            alarm_config = {
                'AlarmName': alarm_name,
                'ComparisonOperator': comparison_operator,
                'EvaluationPeriods': evaluation_periods,
                'MetricName': metric_name,
                'Namespace': namespace,
                'Period': period,
                'Statistic': statistic,
                'Threshold': threshold,
                'ActionsEnabled': True
            }

            if alarm_description:
                alarm_config['AlarmDescription'] = alarm_description

            if dimensions:
                alarm_config['Dimensions'] = dimensions

            if self.sns_topic_arn:
                alarm_config['AlarmActions'] = [self.sns_topic_arn]

            self.cloudwatch_client.put_metric_alarm(**alarm_config)

            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return True

        except ClientError as e:
            logger.error(f"Failed to create CloudWatch alarm: {e}")
            return False

    def send_pipeline_failure_alert(
        self,
        pipeline_name: str,
        error_message: str,
        duration: float,
        context: Optional[Dict[str, Any]] = None
    ):
        """Send alert for pipeline failure.

        Args:
            pipeline_name: Name of the failed pipeline
            error_message: Error message
            duration: Pipeline duration before failure
            context: Additional context
        """
        alert_context = {
            "Pipeline": pipeline_name,
            "Duration": f"{duration:.2f}s",
            "Environment": os.getenv("TARGET", "dev"),
            **(context or {})
        }

        self.send_alert(
            title=f"Pipeline Failure: {pipeline_name}",
            message=f"Pipeline failed with error: {error_message}",
            severity=AlertSeverity.CRITICAL,
            context=alert_context
        )

    def send_data_quality_alert(
        self,
        dataset: str,
        quality_score: float,
        threshold: float,
        issues: List[str]
    ):
        """Send alert for data quality issues.

        Args:
            dataset: Dataset name
            quality_score: Quality score (0-100)
            threshold: Quality threshold
            issues: List of quality issues
        """
        severity = (
            AlertSeverity.CRITICAL if quality_score < threshold * 0.5
            else AlertSeverity.HIGH if quality_score < threshold * 0.75
            else AlertSeverity.MEDIUM
        )

        self.send_alert(
            title=f"Data Quality Alert: {dataset}",
            message=f"Quality score {quality_score:.1f} is below threshold {threshold}",
            severity=severity,
            context={
                "Dataset": dataset,
                "Quality Score": quality_score,
                "Threshold": threshold,
                "Issues": ", ".join(issues)
            }
        )

    def send_freshness_alert(
        self,
        dataset: str,
        hours_old: float,
        sla_hours: float,
        last_update: datetime
    ):
        """Send alert for stale data.

        Args:
            dataset: Dataset name
            hours_old: Age of data in hours
            sla_hours: SLA threshold in hours
            last_update: Last update timestamp
        """
        severity = (
            AlertSeverity.CRITICAL if hours_old > sla_hours * 2
            else AlertSeverity.HIGH
        )

        self.send_alert(
            title=f"Stale Data Alert: {dataset}",
            message=f"Data is {hours_old:.1f} hours old, exceeding SLA of {sla_hours} hours",
            severity=severity,
            context={
                "Dataset": dataset,
                "Age (hours)": f"{hours_old:.1f}",
                "SLA (hours)": sla_hours,
                "Last Update": last_update.isoformat()
            }
        )


# Singleton instance
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get or create the global AlertManager instance."""
    global _alert_manager

    if _alert_manager is None:
        _alert_manager = AlertManager()

    return _alert_manager
