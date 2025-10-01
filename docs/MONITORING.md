# OSAA Data Pipeline Monitoring Guide

## Overview

This guide covers the comprehensive monitoring and observability infrastructure for the OSAA data pipeline, including metrics collection, alerting, data freshness tracking, and execution monitoring.

## Table of Contents

1. [Architecture](#architecture)
2. [Components](#components)
3. [Setup](#setup)
4. [Metrics](#metrics)
5. [Alerting](#alerting)
6. [Data Freshness](#data-freshness)
7. [Dashboards](#dashboards)
8. [Troubleshooting](#troubleshooting)

## Architecture

The monitoring system consists of four main components:

```
┌─────────────────────────────────────────────────────────┐
│                    OSAA Pipeline                        │
├─────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Metrics    │  │   Alerting   │  │  Execution   │  │
│  │  Collection  │  │    System    │  │   Tracking   │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                 │                  │          │
│         ▼                 ▼                  ▼          │
│  ┌──────────────────────────────────────────────────┐  │
│  │              CloudWatch / SNS / SES              │  │
│  └──────────────────────────────────────────────────┘  │
│         │                 │                  │          │
│         ▼                 ▼                  ▼          │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐     │
│  │Dashboard │      │  Slack   │      │  Email   │     │
│  └──────────┘      └──────────┘      └──────────┘     │
└─────────────────────────────────────────────────────────┘
```

## Components

### 1. PipelineMetrics (`src/pipeline/monitoring.py`)

Collects and publishes metrics to AWS CloudWatch.

**Key Metrics:**
- Pipeline execution (success/failure, duration, rows processed)
- Model execution times and row counts
- Data freshness (hours since last update)
- Quality scores
- Error tracking
- S3 upload metrics (size, duration, throughput)

**Usage:**

```python
from pipeline.monitoring import get_metrics

metrics = get_metrics(environment="prod")

# Log pipeline run
metrics.log_pipeline_run(
    status="success",
    duration=123.45,
    rows_processed=10000,
    pipeline_name="ingest"
)

# Log model execution
metrics.log_model_execution(
    model_name="wdi_indicators",
    duration=45.2,
    row_count=5000,
    status="success"
)

# Log data freshness
metrics.log_data_freshness(
    dataset="wdi_indicators",
    last_update=datetime.utcnow(),
    freshness_hours=2.5
)
```

### 2. FreshnessMonitor (`src/pipeline/freshness_monitor.py`)

Monitors data freshness by checking last update timestamps against SLA thresholds.

**Features:**
- Automatic timestamp detection from common columns
- Configurable SLA thresholds per dataset
- Freshness reports
- Integration with alerting system

**Usage:**

```python
from pipeline.freshness_monitor import FreshnessMonitor

monitor = FreshnessMonitor()

# Set custom SLA threshold
monitor.set_sla_threshold("wdi_indicators", hours=24)

# Check specific dataset
is_fresh, hours_old, last_update = monitor.check_dataset_freshness(
    dataset_name="wdi_indicators",
    schema="sources",
    table="wdi_indicators"
)

# Check all datasets
results = monitor.check_all_datasets()

# Generate report
print(monitor.generate_freshness_report())
```

### 3. AlertManager (`src/pipeline/alerting.py`)

Multi-channel alerting system supporting Slack, email (AWS SES), and SNS.

**Alert Severity Levels:**
- CRITICAL: Immediate attention required
- HIGH: Urgent issue
- MEDIUM: Notable issue
- LOW: Minor issue
- INFO: Informational

**Usage:**

```python
from pipeline.alerting import get_alert_manager, AlertSeverity

alert_manager = get_alert_manager()

# Send custom alert
alert_manager.send_alert(
    title="Data Quality Issue",
    message="Quality score dropped below threshold",
    severity=AlertSeverity.HIGH,
    context={"dataset": "wdi_indicators", "score": 75}
)

# Send pipeline failure alert
alert_manager.send_pipeline_failure_alert(
    pipeline_name="ingest",
    error_message="S3 upload failed",
    duration=123.45,
    context={"files_processed": 5}
)

# Send freshness alert
alert_manager.send_freshness_alert(
    dataset="wdi_indicators",
    hours_old=30,
    sla_hours=24,
    last_update=datetime.utcnow() - timedelta(hours=30)
)
```

### 4. ExecutionTracker (`src/pipeline/execution_tracker.py`)

Tracks detailed execution metadata in DuckDB for historical analysis.

**Tracked Information:**
- Pipeline runs (timing, status, configuration)
- Model executions (per-model metrics)
- Data lineage (source → target relationships)
- Custom metrics
- Git commit/branch information

**Usage:**

```python
from pipeline.execution_tracker import ExecutionTracker

tracker = ExecutionTracker()

# Start pipeline run
run_id = tracker.start_pipeline_run(
    pipeline_name="ingest",
    environment="prod",
    config={"key": "value"}
)

# Track model execution
tracker.track_model_execution(
    model_name="wdi_indicators",
    model_type="FULL",
    start_time=start,
    end_time=end,
    status="success",
    rows_produced=5000
)

# Track lineage
tracker.track_lineage(
    source_table="raw.wdi",
    target_table="sources.wdi_indicators",
    transformation_type="TRANSFORMATION",
    row_count=5000
)

# End pipeline run
tracker.end_pipeline_run(
    status="success",
    rows_processed=10000,
    models_executed=3
)
```

## Setup

### 1. Configure Environment Variables

Add these to your `.env` file:

```bash
# Slack Integration (optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Email Alerts (optional)
ALERT_EMAIL_FROM=noreply@osaa-pipeline.com
ALERT_EMAIL_TO=admin@example.com,ops@example.com

# SNS Topic (created by setup script)
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789:osaa-pipeline-alerts-prod
```

### 2. Initialize Monitoring Infrastructure

Run the setup script to create CloudWatch alarms and dashboards:

```bash
# Setup for production
python scripts/setup_monitoring.py \
    --environment prod \
    --region us-east-1 \
    --email admin@example.com

# Setup for development
python scripts/setup_monitoring.py \
    --environment dev \
    --email dev@example.com

# Create dashboard only (skip alarms)
python scripts/setup_monitoring.py \
    --environment prod \
    --dashboard-only
```

This creates:
- SNS topic for alerts
- Email subscription (requires confirmation)
- CloudWatch alarms for:
  - Pipeline failures
  - Stale data (>26 hours old)
  - Low quality scores (<80)
  - High error rates (>5 per 5 min)
- CloudWatch dashboard

### 3. Verify Email Subscription

After running setup, check your email for an SNS subscription confirmation and click the link.

## Metrics

### Pipeline Metrics

| Metric | Description | Unit | Typical Values |
|--------|-------------|------|----------------|
| `PipelineSuccess` | Pipeline success (1) or failure (0) | Count | 0 or 1 |
| `PipelineDuration` | Pipeline execution time | Seconds | 60-300 |
| `RowsProcessed` | Total rows processed | Count | 1000-1000000 |
| `PipelineFailure` | Pipeline failure count | Count | 0 |
| `PipelineError` | Error count | Count | 0-10 |

### Model Metrics

| Metric | Description | Unit | Typical Values |
|--------|-------------|------|----------------|
| `ModelExecutionTime` | Model execution time | Seconds | 5-120 |
| `ModelRowCount` | Rows in model output | Count | 100-100000 |
| `ModelSuccess` | Model success (1) or failure (0) | Count | 0 or 1 |

### Data Quality Metrics

| Metric | Description | Unit | Typical Values |
|--------|-------------|------|----------------|
| `DataFreshnessHours` | Hours since last update | None | 0-24 |
| `DataQualityScore` | Quality score (0-100) | None | 80-100 |
| `LastUpdateTimestamp` | Timestamp of last update | None | Unix epoch |

### S3 Metrics

| Metric | Description | Unit | Typical Values |
|--------|-------------|------|----------------|
| `S3UploadSizeMB` | Size of uploaded data | None | 1-1000 |
| `S3UploadDuration` | Upload duration | Seconds | 1-60 |
| `S3UploadThroughput` | Upload speed | None | 1-100 MB/s |

### Dimensions

All metrics include these dimensions:
- `Environment`: dev/qa/prod
- Additional context-specific dimensions (e.g., `PipelineName`, `ModelName`, `Dataset`)

## Alerting

### Alert Channels

1. **Slack** - Real-time notifications with color-coded severity
2. **Email (AWS SES)** - Detailed HTML emails
3. **SNS** - Integration with other AWS services

### Configured Alarms

#### Pipeline Failure Alarm
- **Trigger**: Any pipeline failure
- **Evaluation**: 1 period of 5 minutes
- **Action**: SNS notification → Email/Slack

#### Data Freshness Alarm
- **Trigger**: Data older than 26 hours
- **Evaluation**: 2 consecutive periods of 1 hour
- **Action**: SNS notification → Email/Slack

#### Quality Score Alarm
- **Trigger**: Quality score below 80
- **Evaluation**: 1 period of 5 minutes
- **Action**: SNS notification → Email/Slack

#### Error Rate Alarm
- **Trigger**: More than 5 errors per 5 minutes
- **Evaluation**: 2 consecutive periods of 5 minutes
- **Action**: SNS notification → Email/Slack

### Custom Alerts

Send custom alerts programmatically:

```python
from pipeline.alerting import get_alert_manager, AlertSeverity

alert_manager = get_alert_manager()

alert_manager.send_alert(
    title="Custom Alert",
    message="Something requires attention",
    severity=AlertSeverity.MEDIUM,
    context={"key": "value"},
    channels=["slack", "email"]  # Optional: specify channels
)
```

## Data Freshness

### Checking Freshness

Use the freshness check utility:

```bash
# Check freshness (no alerts)
python scripts/check_freshness.py

# Check with detailed report
python scripts/check_freshness.py --verbose

# Check and send alerts for stale data
python scripts/check_freshness.py --send-alerts

# Fail if data is stale (useful in CI/CD)
python scripts/check_freshness.py --fail-on-stale
```

### SLA Thresholds

Default SLA is 24 hours. Customize per dataset:

```python
from pipeline.freshness_monitor import FreshnessMonitor

monitor = FreshnessMonitor()

# Set custom thresholds
monitor.set_sla_threshold("wdi_indicators", hours=12)
monitor.set_sla_threshold("sdg_indicators", hours=48)
```

### Freshness Report Example

```
================================================================================
DATA FRESHNESS REPORT
Generated: 2025-10-01T12:00:00Z
================================================================================

wdi_indicators: ✓ FRESH
  Last Update: 2025-10-01T10:30:00
  Age: 1.5 hours
  SLA: 24 hours

sdg_indicators: ✗ STALE
  Last Update: 2025-09-29T08:00:00
  Age: 52.0 hours
  SLA: 24 hours

================================================================================
```

## Dashboards

### CloudWatch Dashboard

Access your environment-specific dashboard:

```
https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=osaa-pipeline-{environment}
```

**Dashboard Panels:**
1. Pipeline Success Rate (24h)
2. Pipeline Duration
3. Rows Processed per Hour
4. Data Freshness by Dataset
5. Data Quality Scores
6. Error Count
7. Model Execution Times
8. S3 Upload Volume
9. S3 Upload Throughput
10. Recent Errors (log insights)

### Custom Dashboards

Modify `monitoring/cloudwatch_dashboard.json` and re-run:

```bash
python scripts/setup_monitoring.py --environment prod --dashboard-only
```

## Execution History

### Query Execution Data

```python
from pipeline.execution_tracker import ExecutionTracker

tracker = ExecutionTracker()

# Get recent runs
recent_runs = tracker.get_recent_runs(limit=10)

for run in recent_runs:
    print(f"{run['run_id']}: {run['status']} - {run['duration_seconds']}s")

# Get specific run details
run_summary = tracker.get_run_summary(run_id="...")
```

### SQL Queries

Query execution tables directly:

```sql
-- Recent pipeline runs
SELECT
    run_id,
    pipeline_name,
    start_time,
    duration_seconds,
    status,
    rows_processed
FROM _tracking.pipeline_runs
ORDER BY start_time DESC
LIMIT 10;

-- Model performance over time
SELECT
    model_name,
    AVG(duration_seconds) as avg_duration,
    AVG(rows_produced) as avg_rows,
    COUNT(*) as execution_count
FROM _tracking.model_executions
WHERE start_time > CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY model_name
ORDER BY avg_duration DESC;

-- Data lineage
SELECT
    source_table,
    target_table,
    transformation_type,
    SUM(row_count) as total_rows
FROM _tracking.data_lineage
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY source_table, target_table, transformation_type;
```

## Integration with Pipeline

The monitoring system is automatically integrated into the ingestion pipeline. For custom pipelines:

```python
from pipeline.monitoring import get_metrics, MetricsContext
from pipeline.execution_tracker import ExecutionTracker
from pipeline.alerting import get_alert_manager

# Initialize
metrics = get_metrics(environment="prod")
tracker = ExecutionTracker()
alert_manager = get_alert_manager()

# Start tracking
tracker.start_pipeline_run(
    pipeline_name="custom_pipeline",
    environment="prod"
)

try:
    # Your pipeline code here
    with MetricsContext(metrics, "my_operation") as ctx:
        # Do work
        result = process_data()
        ctx.set_row_count(len(result))

    # Log success
    metrics.log_pipeline_run(
        status="success",
        duration=123.45,
        rows_processed=10000,
        pipeline_name="custom_pipeline"
    )
    tracker.end_pipeline_run(status="success")

except Exception as e:
    # Log failure
    metrics.log_pipeline_run(
        status="failure",
        duration=123.45,
        rows_processed=5000,
        pipeline_name="custom_pipeline",
        error_message=str(e)
    )
    tracker.end_pipeline_run(status="failure", error_message=str(e))

    # Send alert
    alert_manager.send_pipeline_failure_alert(
        pipeline_name="custom_pipeline",
        error_message=str(e),
        duration=123.45
    )
    raise
```

## Troubleshooting

### Metrics Not Appearing in CloudWatch

1. **Check AWS credentials**: Ensure your credentials have CloudWatch PutMetricData permissions
2. **Verify namespace**: Confirm you're using the correct namespace (`OSAA/DataPipeline`)
3. **Check region**: Ensure metrics and dashboards are in the same region
4. **Wait for propagation**: Metrics can take 5-15 minutes to appear

### Alerts Not Sending

1. **Verify SNS topic ARN**: Check `.env` has correct `SNS_TOPIC_ARN`
2. **Confirm email subscription**: Check email and confirm SNS subscription
3. **Test Slack webhook**: Send a test message to verify the webhook URL
4. **Check alarm state**: Verify alarms are in ALARM state in CloudWatch console

### Freshness Check Failing

1. **Check database path**: Ensure `DB_PATH` points to correct DuckDB file
2. **Verify table names**: Confirm tables exist in specified schemas
3. **Check timestamp columns**: Ensure tables have timestamp columns or data
4. **Review SLA thresholds**: May need to adjust thresholds for specific datasets

### Missing Execution History

1. **Check tracker initialization**: Ensure `ExecutionTracker` is initialized before pipeline runs
2. **Verify database path**: Confirm tracker is writing to correct database
3. **Check table creation**: Tables should be in `_tracking` schema
4. **Review permissions**: Ensure write permissions on database file

### High Error Rates

1. **Check recent errors**: Review CloudWatch Logs or execution tracker
2. **Verify data sources**: Ensure source data is accessible and valid
3. **Check S3 permissions**: Verify read/write permissions on S3 buckets
4. **Review pipeline logs**: Check application logs for detailed error messages

## Best Practices

1. **Monitor regularly**: Check dashboards daily, especially after deployments
2. **Set appropriate thresholds**: Adjust alarm thresholds based on your SLAs
3. **Test alerting**: Periodically test alert channels to ensure they're working
4. **Review execution history**: Analyze trends to identify performance issues
5. **Document incidents**: Keep track of alerts and resolutions for future reference
6. **Update SLA thresholds**: Adjust freshness thresholds as data patterns change
7. **Clean up old data**: Periodically archive or delete old execution tracking data

## Additional Resources

- [AWS CloudWatch Documentation](https://docs.aws.amazon.com/cloudwatch/)
- [AWS SNS Documentation](https://docs.aws.amazon.com/sns/)
- [Slack Incoming Webhooks](https://api.slack.com/messaging/webhooks)
- [DuckDB Documentation](https://duckdb.org/docs/)

## Support

For issues or questions about monitoring:
1. Check this documentation
2. Review CloudWatch Logs
3. Contact the data engineering team
4. File an issue in the project repository
