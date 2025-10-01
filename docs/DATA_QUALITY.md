# Data Quality Framework

## Overview

The UN-OSAA data quality framework provides comprehensive validation, monitoring, and reporting capabilities for indicator datasets (SDG, OPRI, WDI). This framework ensures data integrity, completeness, and reliability throughout the data pipeline.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Quality Framework                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. Pre-Upload Validation (Ingest Pipeline)                      │
│     └── Real-time validation before S3 upload                    │
│                                                                   │
│  2. SQLMesh Audits (Post-Load Validation)                        │
│     ├── indicators_not_null.sql                                  │
│     ├── indicators_unique_grain.sql                              │
│     ├── indicators_value_ranges.sql                              │
│     ├── indicators_referential_integrity.sql                     │
│     ├── indicators_data_freshness.sql                            │
│     └── indicators_completeness.sql                              │
│                                                                   │
│  3. Quality Metrics Tracking                                     │
│     └── Continuous monitoring and scoring                        │
│                                                                   │
│  4. Quality Reporting                                            │
│     └── Automated report generation (HTML, JSON, Console)        │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Data Quality Dimensions

### 1. Accuracy
- **Validation**: Value range checks for years (1960-2030)
- **Validation**: Extreme value detection (values > 1e12)
- **Implementation**: `indicators_value_ranges.sql`

### 2. Completeness
- **Validation**: Not null checks on critical columns (indicator_id, country_id, year)
- **Validation**: Country coverage monitoring (minimum 10 countries per indicator)
- **Validation**: Time series completeness (minimum 5 years per indicator)
- **Implementation**:
  - `indicators_not_null.sql`
  - `indicators_completeness.sql`

### 3. Consistency
- **Validation**: Unique grain verification (indicator_id, country_id, year)
- **Validation**: Referential integrity between data and label tables
- **Implementation**:
  - `indicators_unique_grain.sql`
  - `indicators_referential_integrity.sql`

### 4. Timeliness
- **Validation**: Data freshness checks (alert if data > 1 year old)
- **Validation**: Time series gap detection
- **Implementation**: `indicators_data_freshness.sql`

### 5. Validity
- **Validation**: Schema validation
- **Validation**: Data type validation
- **Validation**: Duplicate detection
- **Implementation**: Pre-upload validation in `src/pipeline/ingest/run.py`

## SQLMesh Audits

### indicators_not_null.sql
**Purpose**: Ensure critical columns never contain null values

**Critical Columns**:
- `indicator_id`: Required for all datasets
- `country_id`: Required for all datasets
- `year`: Required for SDG and OPRI datasets

**Failure Condition**: Any null values in critical columns

**Example Output**:
```
source_table          | null_count | column_name
----------------------|------------|-------------
sdg.data_national     | 5          | indicator_id
opri.data_national    | 0          | country_id
```

### indicators_unique_grain.sql
**Purpose**: Verify grain uniqueness to prevent duplicate records

**Grain Definition**:
- SDG/OPRI: (indicator_id, country_id, year)
- WDI: (Indicator Code, Country Code)

**Failure Condition**: Any duplicate combinations of grain columns

**Example Output**:
```
source_table          | indicator_id | country_id | year | duplicate_count
----------------------|--------------|------------|------|----------------
sdg.data_national     | SDG_01_01    | USA        | 2020 | 2
```

### indicators_value_ranges.sql
**Purpose**: Validate that values fall within reasonable ranges

**Checks**:
- Year between 1960 and 2030
- Values not exceeding 1e12 (extreme outlier detection)

**Failure Condition**: Values outside reasonable ranges

**Example Output**:
```
source_table          | violation_type     | year | value | issue_description
----------------------|--------------------|------|-------|-------------------
sdg.data_national     | year_out_of_range  | 2035 | 123   | Year after 2030
opri.data_national    | extreme_value      | 2020 | 1e15  | Value exceeds reasonable range
```

### indicators_referential_integrity.sql
**Purpose**: Ensure data tables reference valid metadata/label records

**Checks**:
- SDG data_national → SDG label (indicator_id)
- OPRI data_national → OPRI label (indicator_id)
- WDI csv → WDI series (Indicator Code)

**Failure Condition**: Data records with missing metadata references

**Example Output**:
```
source_table          | violation_type          | indicator_id | affected_records
----------------------|-------------------------|--------------|------------------
sdg.data_national     | missing_indicator_label | SDG_ORPHAN   | 150
```

### indicators_data_freshness.sql
**Purpose**: Monitor data currency and detect stale data

**Checks**:
- Most recent year vs current year (alert if > 1 year behind)
- Critical alert if > 2 years behind
- Per-indicator freshness (detect indicators with old data)

**Failure Condition**: Data more than 1 year old

**Example Output**:
```
source_table          | most_recent_year | years_behind | status
----------------------|------------------|--------------|---------------------------
sdg.data_national     | 2022             | 2            | CRITICAL: Data more than 2 years old
```

### indicators_completeness.sql
**Purpose**: Monitor data coverage and quality

**Checks**:
- Indicators with low country coverage (< 10 countries)
- Indicators with sparse time series (< 5 years)
- High null value rates (> 50%)
- Dataset-level coverage statistics

**Failure Condition**:
- Country coverage < 10
- Year coverage < 5
- Null rate > 50%

**Example Output**:
```
source_table          | completeness_check      | indicator_id | country_count | issue
----------------------|-------------------------|--------------|---------------|-------
sdg.data_national     | low_country_coverage    | SDG_SPARSE   | 5             | Indicator has fewer than 10 countries
opri.data_national    | high_null_rate          | OPRI_NULL    | 50            | Null value rate: 75%
```

## Running Audits

### Manual Execution

```bash
# Run all audits
sqlmesh audit

# Run specific audit
sqlmesh audit indicators_not_null

# Run audits for specific model
sqlmesh audit --model sdg.data_national
```

### Automated Execution

Audits run automatically during:
1. SQLMesh plan execution
2. Model materialization
3. Scheduled pipeline runs

### Audit Configuration

Configure audit behavior in `sqlMesh/config.yaml`:

```yaml
audits:
  # Fail on audit errors (default: false)
  strict_mode: false

  # Audit execution mode
  mode: parallel  # or sequential

  # Timeout for audit queries (seconds)
  timeout: 300
```

## Pre-Upload Validation

The ingest pipeline includes real-time validation before S3 upload.

### Implementation

Located in `src/pipeline/ingest/run.py`:

```python
def validate_data_quality(self, table_name: str) -> Tuple[bool, List[str]]:
    """
    Validates:
    - Table exists and has data
    - No nulls in critical columns
    - Year values in range (1960-2030)
    - No duplicate grain combinations
    """
```

### Validation Flow

```
CSV File → DuckDB Table → Validation → S3 Upload
                              ↓
                         Pass/Fail + Issues
```

### Example Output

```
INFO: Running data quality validation for source.sdg_data_national
INFO: Table source.sdg_data_national has 15000 rows
INFO: Columns in source.sdg_data_national: ['indicator_id', 'country_id', 'year', 'value']
WARNING: Null values detected in value: 150
WARNING: Found 2 duplicate records based on grain columns
WARNING: Data quality validation failed for source.sdg_data_national
WARNING: Issues found: 2
  - Column value has 150 null values (1.00%)
  - Found 2 duplicate records based on grain columns
WARNING: Proceeding with upload despite validation issues
```

## Quality Metrics

### Quality Score Calculation

Overall quality score (0-100) is calculated as:

```
Quality Score = (Completeness × 0.5) + (Non-Null Rate × 0.3) + (Uniqueness × 0.2)

Where:
- Completeness = % of non-null values in value column
- Non-Null Rate = % of non-null values in critical columns
- Uniqueness = 100 - (duplicate_rate × 10)
```

### Score Interpretation

- **90-100**: Excellent - Production ready
- **80-89**: Good - Minor issues, acceptable
- **60-79**: Fair - Moderate issues, needs attention
- **0-59**: Poor - Critical issues, requires immediate action

### Metrics Tracked

For each dataset:
- Total records
- Total indicators
- Total countries
- Total years
- Completeness percentage
- Null rate percentage
- Duplicate count
- Year range (min, max)
- Quality score
- Issues list

### Using the Metrics Module

```python
from pipeline.quality_metrics import QualityMetrics

# Initialize
qm = QualityMetrics()

# Calculate metrics for single dataset
metrics = qm.calculate_dataset_metrics('sdg.data_national')
print(f"Quality Score: {metrics.quality_score}/100")

# Calculate metrics for all datasets
all_metrics = qm.calculate_all_metrics()

# Export to JSON
qm.export_metrics_json(all_metrics, 'reports/quality_metrics.json')
```

## Quality Reporting

### Command-Line Interface

```bash
# Console report (default)
python scripts/data_quality_report.py

# HTML report
python scripts/data_quality_report.py --format html --output reports/quality_report.html

# JSON export
python scripts/data_quality_report.py --format json --output reports/quality_metrics.json
```

### Console Report Output

```
================================================================================
UN-OSAA DATA QUALITY REPORT
================================================================================

Generated: 2025-10-01 10:30:00

Overall Quality Score: 87.5/100
Total Records: 450,000
Datasets Monitored: 3
Issues Detected: 5

--------------------------------------------------------------------------------

Dataset: sdg.data_national
Quality Score: 92/100

Metrics:
  Total Records: 150,000
  Indicators: 250
  Countries: 195
  Years: 15
  Completeness: 95.0%
  Null Rate: 2.5%
  Duplicates: 0
  Year Range: 2008-2023

No issues detected
```

### HTML Report

The HTML report provides:
- Executive summary with key metrics
- Visual quality scores with color coding
- Detailed dataset breakdowns
- Interactive issue tracking
- Responsive design for mobile/desktop

Preview:
```
┌──────────────────────────────────────────────────┐
│         UN-OSAA Data Quality Report              │
│                                                  │
│  Overall Score: 87.5    Total Records: 450K     │
│  Datasets: 3            Issues: 5                │
│                                                  │
│  ┌────────────────────────────────────┐         │
│  │ sdg.data_national         [92/100] │         │
│  │ ├─ Records: 150,000                │         │
│  │ ├─ Completeness: 95%               │         │
│  │ └─ Issues: None                    │         │
│  └────────────────────────────────────┘         │
└──────────────────────────────────────────────────┘
```

### JSON Export

Structure:
```json
{
  "sdg.data_national": {
    "dataset_name": "sdg.data_national",
    "timestamp": "2025-10-01T10:30:00",
    "total_records": 150000,
    "total_indicators": 250,
    "total_countries": 195,
    "total_years": 15,
    "completeness_percentage": 95.0,
    "null_rate_percentage": 2.5,
    "duplicate_count": 0,
    "year_range_min": 2008,
    "year_range_max": 2023,
    "quality_score": 92.0,
    "issues": []
  }
}
```

## Alerting and Monitoring

### Alert Levels

1. **INFO**: Normal operations, no action needed
2. **WARNING**: Minor issues, monitor closely
3. **ERROR**: Data quality violations, investigate
4. **CRITICAL**: Severe issues, immediate action required

### Alert Triggers

| Condition | Level | Action |
|-----------|-------|--------|
| Quality score < 60 | CRITICAL | Block pipeline, notify team |
| Quality score 60-79 | ERROR | Continue with warning, investigate |
| Null rate > 20% | WARNING | Monitor, plan cleanup |
| Duplicates > 0 | ERROR | Investigate and deduplicate |
| Data > 2 years old | CRITICAL | Refresh data source |
| Missing references | ERROR | Update metadata tables |

### Integration with Monitoring Systems

#### CloudWatch (AWS)

```python
import boto3

cloudwatch = boto3.client('cloudwatch')

# Publish quality score metric
cloudwatch.put_metric_data(
    Namespace='OSAA/DataQuality',
    MetricData=[{
        'MetricName': 'QualityScore',
        'Value': quality_score,
        'Unit': 'None',
        'Dimensions': [
            {'Name': 'Dataset', 'Value': 'sdg.data_national'}
        ]
    }]
)
```

#### Datadog

```python
from datadog import statsd

# Send quality metrics
statsd.gauge('osaa.data_quality.score', quality_score, tags=['dataset:sdg'])
statsd.gauge('osaa.data_quality.null_rate', null_rate, tags=['dataset:sdg'])
```

## Best Practices

### 1. Regular Monitoring
- Run quality reports daily
- Review metrics weekly
- Investigate trends monthly

### 2. Threshold Management
- Set appropriate thresholds per dataset
- Adjust based on historical performance
- Document threshold rationale

### 3. Issue Remediation
- Prioritize critical issues
- Track resolution time
- Document root causes

### 4. Continuous Improvement
- Analyze recurring issues
- Update validation rules
- Enhance data sources

### 5. Documentation
- Document data quality rules
- Maintain issue registry
- Share insights with stakeholders

## Troubleshooting

### Audit Failures

**Problem**: Audit returns unexpected results

**Solution**:
1. Verify data has been loaded: `SELECT COUNT(*) FROM sdg.data_national`
2. Check audit query syntax: `sqlmesh test indicators_not_null`
3. Review audit logs: `tail -f logs/sqlmesh.log`

### Validation Errors

**Problem**: Pre-upload validation fails

**Solution**:
1. Check table schema: `DESCRIBE source.table_name`
2. Verify column names (case-sensitive)
3. Review validation logic in `run.py`
4. Examine data sample: `SELECT * FROM table LIMIT 10`

### Low Quality Scores

**Problem**: Consistently low quality scores

**Solution**:
1. Identify specific issues: Run detailed quality report
2. Prioritize by impact: Focus on high-volume indicators
3. Trace to source: Check upstream data providers
4. Implement fixes: Update ETL or source data
5. Monitor improvements: Track score trends

### Performance Issues

**Problem**: Audits taking too long

**Solution**:
1. Add indexes on grain columns
2. Partition large tables by year
3. Run audits in parallel
4. Sample large datasets for validation

## Future Enhancements

### Planned Features

1. **Machine Learning Anomaly Detection**
   - Detect unusual patterns in indicator values
   - Predict expected ranges based on historical data

2. **Real-time Dashboards**
   - Live quality metrics visualization
   - Alert notifications

3. **Data Quality SLAs**
   - Define service level agreements per dataset
   - Track SLA compliance

4. **Automated Remediation**
   - Auto-fix common issues
   - Suggest corrections for review

5. **Historical Trend Analysis**
   - Track quality metrics over time
   - Identify degradation patterns

## References

- [SQLMesh Documentation](https://sqlmesh.readthedocs.io/)
- [DuckDB Data Quality Patterns](https://duckdb.org/docs/guides/data_quality)
- [Data Quality Dimensions (DAMA)](https://www.dama.org/)

## Support

For questions or issues with the data quality framework:

1. Check this documentation
2. Review audit logs
3. Contact the data engineering team
4. Open an issue in the project repository

---

**Last Updated**: 2025-10-01
**Version**: 1.0.0
**Maintained By**: UN-OSAA Data Engineering Team
