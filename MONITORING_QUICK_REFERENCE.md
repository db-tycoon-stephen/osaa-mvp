# Monitoring Quick Reference Card

## One-Time Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment variables in .env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
ALERT_EMAIL_FROM=noreply@osaa-pipeline.com
ALERT_EMAIL_TO=admin@example.com,ops@example.com

# Setup monitoring infrastructure
python scripts/setup_monitoring.py \
    --environment prod \
    --region us-east-1 \
    --email your-email@example.com

# Confirm email subscription (check inbox)
```

## Daily Operations

### Check Data Freshness

```bash
# Quick check
python scripts/check_freshness.py

# Detailed report
python scripts/check_freshness.py --verbose

# Check and send alerts
python scripts/check_freshness.py --send-alerts
```

### View Metrics

```bash
# Access CloudWatch Dashboard
# https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=osaa-pipeline-prod

# Query execution history
python -c "
from pipeline.execution_tracker import ExecutionTracker
tracker = ExecutionTracker()
runs = tracker.get_recent_runs(limit=10)
for run in runs:
    print(f'{run[\"start_time\"]}: {run[\"status\"]} - {run[\"duration_seconds\"]:.1f}s')
"
```

## Common Code Patterns

### Log Pipeline Metrics

```python
from pipeline.monitoring import get_metrics

metrics = get_metrics(environment="prod")

metrics.log_pipeline_run(
    status="success",
    duration=123.45,
    rows_processed=10000,
    pipeline_name="my_pipeline"
)
```

### Send Alert

```python
from pipeline.alerting import get_alert_manager, AlertSeverity

alert_manager = get_alert_manager()

alert_manager.send_alert(
    title="Data Quality Issue",
    message="Quality score dropped below threshold",
    severity=AlertSeverity.HIGH,
    context={"dataset": "wdi", "score": 75}
)
```

### Track Execution

```python
from pipeline.execution_tracker import ExecutionTracker

tracker = ExecutionTracker()

# Start tracking
tracker.start_pipeline_run(
    pipeline_name="my_pipeline",
    environment="prod"
)

# Track model
tracker.track_model_execution(
    model_name="my_model",
    model_type="FULL",
    start_time=start,
    end_time=end,
    status="success",
    rows_produced=5000
)

# End tracking
tracker.end_pipeline_run(
    status="success",
    rows_processed=10000
)
```

### Check Freshness

```python
from pipeline.freshness_monitor import FreshnessMonitor

monitor = FreshnessMonitor()

# Check specific dataset
is_fresh, hours_old, last_update = monitor.check_dataset_freshness(
    dataset_name="wdi_indicators",
    schema="sources",
    table="wdi_indicators"
)

# Check all datasets
results = monitor.check_all_datasets()

# Get stale datasets
stale = monitor.get_stale_datasets()
```

## Key Files

| File | Purpose |
|------|---------|
| `src/pipeline/monitoring.py` | Metrics collection |
| `src/pipeline/alerting.py` | Alert sending |
| `src/pipeline/freshness_monitor.py` | Freshness checks |
| `src/pipeline/execution_tracker.py` | Execution history |
| `docs/MONITORING.md` | Complete documentation |

## CloudWatch Metrics

| Metric | Unit | Typical Value |
|--------|------|---------------|
| PipelineSuccess | Count | 1 (success) or 0 (fail) |
| PipelineDuration | Seconds | 60-300 |
| RowsProcessed | Count | 1K-1M |
| DataFreshnessHours | None | 0-24 |
| DataQualityScore | None | 80-100 |

## Alarm Thresholds

| Alarm | Threshold | Evaluation |
|-------|-----------|------------|
| Pipeline Failure | >0 failures | 1 period (5 min) |
| Data Freshness | >26 hours | 2 periods (1 hour) |
| Quality Score | <80 | 1 period (5 min) |
| Error Rate | >5 errors | 2 periods (5 min) |

## Troubleshooting

### Metrics Not Showing
1. Check AWS credentials
2. Wait 5-15 minutes for propagation
3. Verify namespace: `OSAA/DataPipeline`

### Alerts Not Sending
1. Confirm SNS subscription (check email)
2. Test Slack webhook URL
3. Check alarm state in CloudWatch

### Freshness Check Fails
1. Verify database path
2. Check table exists
3. Ensure timestamp columns present

## SQL Queries

### Recent Runs
```sql
SELECT run_id, pipeline_name, start_time, status, duration_seconds
FROM _tracking.pipeline_runs
ORDER BY start_time DESC
LIMIT 10;
```

### Model Performance
```sql
SELECT model_name, AVG(duration_seconds) as avg_time, COUNT(*) as runs
FROM _tracking.model_executions
WHERE start_time > CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY model_name
ORDER BY avg_time DESC;
```

### Data Lineage
```sql
SELECT source_table, target_table, SUM(row_count) as total_rows
FROM _tracking.data_lineage
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY source_table, target_table;
```

## Support

- **Documentation**: [docs/MONITORING.md](docs/MONITORING.md)
- **Implementation**: [MONITORING_IMPLEMENTATION.md](MONITORING_IMPLEMENTATION.md)
- **CloudWatch Console**: https://console.aws.amazon.com/cloudwatch/
- **Project Repository**: https://github.com/UN-OSAA/osaa-mvp

---

For detailed information, see [docs/MONITORING.md](docs/MONITORING.md)
