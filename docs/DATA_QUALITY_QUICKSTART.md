# Data Quality Framework - Quick Start Guide

## 5-Minute Setup

### 1. Run Data Quality Report

```bash
# Console output
python scripts/data_quality_report.py

# HTML report
python scripts/data_quality_report.py --format html --output reports/quality_report.html

# JSON export
python scripts/data_quality_report.py --format json --output reports/metrics.json
```

### 2. Run SQLMesh Audits

```bash
# Run all audits
sqlmesh audit

# Run specific audit
sqlmesh audit indicators_not_null
```

### 3. Check Pipeline Validation

Validation runs automatically during ingestion:

```bash
python -m pipeline.ingest.run
```

Look for logs:
```
INFO: Running data quality validation for source.sdg_data_national
INFO: Data quality validation passed for source.sdg_data_national
```

## Available Audits

| Audit | Purpose | What It Checks |
|-------|---------|----------------|
| `indicators_not_null` | Critical column validation | indicator_id, country_id, year are never null |
| `indicators_unique_grain` | Duplicate detection | (indicator_id, country_id, year) is unique |
| `indicators_value_ranges` | Range validation | Years 1960-2030, values < 1e12 |
| `indicators_referential_integrity` | Reference validation | All data has matching labels/metadata |
| `indicators_data_freshness` | Timeliness check | Data is not more than 1 year old |
| `indicators_completeness` | Coverage monitoring | Sufficient countries and years per indicator |

## Quick Metrics

```python
from pipeline.quality_metrics import QualityMetrics

qm = QualityMetrics()
metrics = qm.calculate_dataset_metrics('sdg.data_national')

print(f"Quality Score: {metrics.quality_score}/100")
print(f"Completeness: {metrics.completeness_percentage}%")
print(f"Null Rate: {metrics.null_rate_percentage}%")
```

## Quality Score Interpretation

- **90-100**: Excellent - Production ready
- **80-89**: Good - Minor issues
- **60-79**: Fair - Needs attention
- **0-59**: Poor - Critical issues

## Common Issues and Fixes

### Issue: Null values in critical columns
```sql
-- Find null records
SELECT * FROM sdg.data_national WHERE indicator_id IS NULL;

-- Fix at source or filter during ingestion
```

### Issue: Duplicate records
```sql
-- Find duplicates
SELECT indicator_id, country_id, year, COUNT(*)
FROM sdg.data_national
GROUP BY indicator_id, country_id, year
HAVING COUNT(*) > 1;

-- Deduplicate
CREATE TABLE sdg.data_national_clean AS
SELECT DISTINCT * FROM sdg.data_national;
```

### Issue: Out-of-range years
```sql
-- Find invalid years
SELECT * FROM sdg.data_national WHERE year < 1960 OR year > 2030;

-- Filter during ingestion or correct at source
```

## Integration with CI/CD

Add to your pipeline:

```bash
# .github/workflows/data_quality.yml
- name: Run Data Quality Checks
  run: |
    python scripts/data_quality_report.py --format json --output reports/quality.json
    sqlmesh audit
```

## Monitoring Dashboard

View the HTML report for visual monitoring:

```bash
# Generate and open report
python scripts/data_quality_report.py --format html --output reports/quality.html
open reports/quality.html
```

## Alert Configuration

Set thresholds in your monitoring system:

```python
# Example: CloudWatch alarm
if quality_score < 60:
    send_alert("CRITICAL: Data quality below threshold")
elif quality_score < 80:
    send_alert("WARNING: Data quality needs attention")
```

## Next Steps

1. Review [Full Documentation](DATA_QUALITY.md)
2. Customize audit thresholds
3. Set up automated reporting
4. Integrate with monitoring systems

## Support

Questions? Check:
- [Full Documentation](DATA_QUALITY.md)
- Audit logs: `logs/sqlmesh.log`
- Pipeline logs: `logs/pipeline.log`
