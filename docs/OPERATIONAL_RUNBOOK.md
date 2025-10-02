# OSAA Data Pipeline - Operational Runbook

## Table of Contents

1. [Overview](#overview)
2. [Common Operations](#common-operations)
3. [Troubleshooting Guide](#troubleshooting-guide)
4. [Emergency Procedures](#emergency-procedures)
5. [Monitoring](#monitoring)
6. [Maintenance Tasks](#maintenance-tasks)
7. [Performance Tuning](#performance-tuning)
8. [Contact Information](#contact-information)

## Overview

This operational runbook provides step-by-step procedures for managing, troubleshooting, and maintaining the OSAA Data Pipeline. It serves as the primary reference for operations teams, developers, and support staff.

### System Architecture

The OSAA Data Pipeline consists of:
- **Ingestion Layer**: Converts CSV files to optimized Parquet format
- **Transformation Layer**: SQLMesh-based data transformations using DuckDB
- **Storage Layer**: S3-based data lake with dev/qa/prod environments
- **Orchestration**: Docker-based execution with CI/CD via GitHub Actions

### Key Components

- **Docker Container**: osaa-mvp (built from Dockerfile)
- **Database Engine**: DuckDB (embedded)
- **Transformation Engine**: SQLMesh with Ibis
- **Storage Backend**: AWS S3
- **CI/CD Platform**: GitHub Actions

## Common Operations

### 1. Running the Pipeline

#### Full Pipeline Execution

```bash
# Build the container (required after code changes)
docker build -t osaa-mvp .

# Run complete pipeline
docker compose run --rm pipeline ingest
docker compose run --rm pipeline etl
```

#### Development Mode Execution

```bash
# Run with your username for isolated development
docker compose run --rm -e USERNAME=your_name pipeline etl
```

#### Selective Execution

```bash
# Run only ingestion
docker compose run --rm pipeline ingest

# Run only transformations
docker compose run --rm pipeline transform

# Run configuration test
docker compose run --rm pipeline config_test
```

### 2. Adding New Datasets

#### Step 1: Prepare Source Data

```bash
# Place CSV file in appropriate directory
cp your_data.csv data/raw/<source_name>/
```

#### Step 2: Create Source Model

Create file: `sqlMesh/models/sources/<source_name>/<model_name>.py`

```python
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int",
    "value": "Decimal",
}

@model(
    "sources.<source_name>",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="Your dataset description"
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    # Model implementation
    pass
```

#### Step 3: Test and Deploy

```bash
# Rebuild container
docker build -t osaa-mvp .

# Test in development
docker compose run --rm -e USERNAME=test pipeline etl

# Deploy to production (via PR merge)
git add .
git commit -m "Add new dataset: <source_name>"
git push origin feature/new-dataset
```

### 3. Reprocessing Data

#### Full Reprocessing

```bash
# Clear existing data (BE CAREFUL!)
aws s3 rm s3://osaa-mvp/dev/staging/ --recursive

# Rerun pipeline
docker compose run --rm pipeline ingest
docker compose run --rm pipeline etl
```

#### Partial Reprocessing

```bash
# Reprocess specific source
docker compose run --rm pipeline etl --model sources.<source_name>
```

### 4. Data Promotion

#### Promote from Dev to QA

```bash
docker compose run --rm pipeline promote --from dev --to qa
```

#### Promote from QA to Production

```bash
# Requires appropriate permissions
docker compose run --rm pipeline promote --from qa --to prod
```

### 5. Schema Updates

#### Adding a Column

1. Update COLUMN_SCHEMA in model file
2. Update column_descriptions in @model decorator
3. Rebuild and test:

```bash
docker build -t osaa-mvp .
docker compose run --rm pipeline etl --model <model_name>
```

#### Changing Data Types

1. Update COLUMN_SCHEMA
2. Add migration logic if needed
3. Test thoroughly before production deployment

## Troubleshooting Guide

### Pipeline Failures

#### Issue: Container Build Fails

**Symptoms**: Docker build command fails with error

**Resolution**:
```bash
# Check Docker daemon is running
docker ps

# Clean Docker cache
docker system prune -a

# Rebuild with no cache
docker build --no-cache -t osaa-mvp .
```

#### Issue: S3 Access Denied

**Symptoms**: Pipeline fails with S3 permission errors

**Resolution**:
```bash
# Verify credentials
cat .env | grep AWS

# Test S3 access
aws s3 ls s3://osaa-mvp/

# Check IAM permissions
aws iam get-user
```

#### Issue: DuckDB Lock Error

**Symptoms**: "Database is locked" error

**Resolution**:
```bash
# Find and kill processes using the database
lsof | grep unosaa_data_pipeline.db

# Remove lock file if exists
rm sqlMesh/unosaa_data_pipeline.db.wal
rm sqlMesh/unosaa_data_pipeline.db.lock
```

### Data Quality Issues

#### Issue: Missing Data

**Symptoms**: Expected data not appearing in output

**Diagnostic Steps**:
```bash
# Check source data
aws s3 ls s3://osaa-mvp/dev/landing/<source>/

# Verify transformation
docker compose run --rm pipeline etl --dry-run

# Check logs
docker compose logs | grep ERROR
```

#### Issue: Duplicate Records

**Symptoms**: Same records appearing multiple times

**Resolution**:
```sql
-- Identify duplicates
SELECT indicator_id, country_id, year, COUNT(*)
FROM master.indicators
GROUP BY indicator_id, country_id, year
HAVING COUNT(*) > 1;

-- Fix in model with DISTINCT
```

#### Issue: Data Type Mismatches

**Symptoms**: Type conversion errors during processing

**Resolution**:
1. Check source data format
2. Update COLUMN_SCHEMA
3. Add explicit type casting in transformation

### Performance Issues

#### Issue: Slow Pipeline Execution

**Symptoms**: Pipeline takes longer than SLA

**Diagnostic**:
```bash
# Profile execution
time docker compose run --rm pipeline etl

# Check resource usage
docker stats

# Review query plans in SQLMesh UI
docker compose --profile ui up ui
```

**Resolution**:
- Increase Docker memory allocation
- Optimize SQL queries
- Use incremental models for large datasets
- Partition data by year/month

## Emergency Procedures

### 1. Pipeline Complete Failure

**Immediate Actions**:

1. **Notify Stakeholders**
   ```bash
   # Send notification
   echo "Pipeline failure detected at $(date)" | mail -s "URGENT: Pipeline Failure" team@un.org
   ```

2. **Capture Diagnostics**
   ```bash
   # Collect logs
   docker compose logs > failure_logs_$(date +%Y%m%d_%H%M%S).txt

   # Check system resources
   df -h
   free -m
   ```

3. **Attempt Recovery**
   ```bash
   # Restart Docker
   docker compose down
   docker compose up -d

   # Retry pipeline
   docker compose run --rm pipeline etl
   ```

### 2. Data Corruption

**Detection**:
```sql
-- Check data integrity
SELECT source, COUNT(*) as record_count
FROM master.indicators
GROUP BY source;

-- Verify critical metrics
SELECT year, COUNT(DISTINCT country_id) as countries
FROM sources.sdg
GROUP BY year
ORDER BY year DESC;
```

**Recovery**:
```bash
# Restore from backup
aws s3 sync s3://osaa-mvp-backup/prod/ s3://osaa-mvp/prod/

# Reprocess from raw data
docker compose run --rm pipeline ingest
docker compose run --rm pipeline etl
```

### 3. Rollback Procedures

#### Code Rollback

```bash
# Identify last working commit
git log --oneline -10

# Rollback to specific commit
git checkout <commit-hash>

# Rebuild and deploy
docker build -t osaa-mvp .
docker compose run --rm pipeline etl
```

#### Data Rollback

```bash
# List available backups
aws s3 ls s3://osaa-mvp-backup/ --recursive | grep $(date +%Y-%m-%d)

# Restore specific backup
aws s3 sync s3://osaa-mvp-backup/<backup-date>/ s3://osaa-mvp/prod/
```

### 4. Incident Response

**Level 1: Minor Issue** (No data loss, <2 hour delay)
- Investigate and fix
- Document in incident log
- No escalation required

**Level 2: Major Issue** (Potential data loss, >2 hour delay)
- Notify team lead immediately
- Begin diagnostic collection
- Prepare incident report

**Level 3: Critical Issue** (Data loss confirmed, >4 hour delay)
- Activate emergency response team
- Notify all stakeholders
- Implement recovery procedures
- Post-incident review required

## Monitoring

### Health Checks

#### Pipeline Health

```bash
# Check last run status
docker compose logs | tail -50 | grep -E "SUCCESS|ERROR|FAILED"

# Verify output exists
aws s3 ls s3://osaa-mvp/prod/staging/master/ --recursive | head -10
```

#### Data Freshness

```sql
-- Check latest data dates
SELECT source, MAX(year) as latest_year, COUNT(*) as record_count
FROM master.indicators
GROUP BY source;

-- Monitor data gaps
SELECT year, COUNT(DISTINCT country_id) as countries
FROM sources.sdg
WHERE year >= 2020
GROUP BY year
ORDER BY year;
```

### Alerting Setup

#### CloudWatch Alarms (AWS)

```bash
# Set up S3 upload monitoring
aws cloudwatch put-metric-alarm \
  --alarm-name "OSAA-Pipeline-S3-Upload" \
  --alarm-description "Alert when S3 uploads fail" \
  --metric-name NumberOfObjectsUploaded \
  --namespace AWS/S3 \
  --statistic Sum \
  --period 3600 \
  --threshold 0 \
  --comparison-operator LessThanThreshold
```

#### GitHub Actions Monitoring

- Check workflow runs: https://github.com/UN-OSAA/osaa-mvp/actions
- Enable email notifications in GitHub settings
- Review failed workflow logs

### Performance Metrics

Track these KPIs:

| Metric | Target | Measurement |
|--------|--------|-------------|
| Pipeline Runtime | <2 hours | GitHub Actions duration |
| Data Freshness | <24 hours | Max lag from source update |
| Error Rate | <1% | Failed runs / total runs |
| Data Completeness | >95% | Non-null critical fields |
| S3 Upload Success | 100% | Successful uploads / attempts |

## Maintenance Tasks

### Daily Tasks

1. **Morning Health Check** (09:00 UTC)
   ```bash
   # Check overnight runs
   docker compose logs --since 24h | grep -E "ERROR|FAILED"

   # Verify data updates
   aws s3 ls s3://osaa-mvp/prod/staging/ --recursive | grep $(date +%Y-%m-%d)
   ```

2. **Data Quality Validation**
   ```sql
   -- Run quality checks
   SELECT * FROM data_quality.daily_checks
   WHERE check_date = CURRENT_DATE;
   ```

### Weekly Tasks

1. **Log Rotation**
   ```bash
   # Archive old logs
   find /var/log/osaa-pipeline -mtime +7 -type f -name "*.log" | \
     xargs tar -czf logs_$(date +%Y%m%d).tar.gz

   # Clean up
   find /var/log/osaa-pipeline -mtime +7 -type f -name "*.log" -delete
   ```

2. **Performance Review**
   ```bash
   # Generate performance report
   docker compose run --rm pipeline performance_report --week
   ```

3. **Backup Verification**
   ```bash
   # Test restore procedure
   aws s3 cp s3://osaa-mvp-backup/test-restore.parquet /tmp/
   duckdb -c "SELECT COUNT(*) FROM '/tmp/test-restore.parquet'"
   ```

### Monthly Tasks

1. **Capacity Planning**
   ```bash
   # Check storage usage
   aws s3api list-objects --bucket osaa-mvp \
     --query "sum(Contents[].Size)" --output text

   # Review growth trends
   docker compose run --rm pipeline storage_report --monthly
   ```

2. **Security Updates**
   ```bash
   # Update base images
   docker pull python:3.11-slim

   # Update dependencies
   pip list --outdated
   ```

3. **Documentation Review**
   - Update this runbook with new procedures
   - Review and update data catalog
   - Update team contact information

### Quarterly Tasks

1. **Disaster Recovery Test**
   ```bash
   # Full DR drill
   ./scripts/disaster_recovery_test.sh
   ```

2. **Performance Baseline**
   ```bash
   # Benchmark pipeline performance
   docker compose run --rm pipeline benchmark --full
   ```

3. **Access Review**
   ```bash
   # Audit S3 access
   aws s3api get-bucket-acl --bucket osaa-mvp

   # Review IAM policies
   aws iam list-attached-user-policies --user-name osaa-pipeline
   ```

## Performance Tuning

### DuckDB Optimization

```sql
-- Set memory limit
SET memory_limit = '8GB';

-- Enable parallel execution
SET threads = 4;

-- Optimize for analytics
SET enable_progress_bar = false;
SET preserve_insertion_order = false;
```

### SQLMesh Optimization

```python
# Use incremental models for large datasets
@model(
    "sources.large_dataset",
    kind="INCREMENTAL_BY_TIME_RANGE",
    time_column="date",
    ...
)

# Add indexes for frequently queried columns
@model(
    ...
    pre_statements=["CREATE INDEX idx_country ON TABLE (country_id)"]
)
```

### Docker Optimization

```yaml
# docker-compose.yml adjustments
services:
  pipeline:
    mem_limit: 8g
    cpus: '4.0'
    environment:
      - PYTHONUNBUFFERED=1
      - DUCKDB_MEMORY_LIMIT=6GB
```

### S3 Transfer Optimization

```bash
# Use multipart uploads for large files
aws configure set s3.max_concurrent_requests 10
aws configure set s3.max_queue_size 10000
aws configure set s3.multipart_threshold 64MB
aws configure set s3.multipart_chunksize 16MB
```

## Appendix

### A. Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| AWS_ACCESS_KEY_ID | AWS access key | AKIA... |
| AWS_SECRET_ACCESS_KEY | AWS secret key | wJal... |
| AWS_DEFAULT_REGION | AWS region | us-east-1 |
| S3_BUCKET | S3 bucket name | osaa-mvp |
| ENVIRONMENT | Deployment environment | dev/qa/prod |
| USERNAME | Developer username | john_doe |
| PYTHONPATH | Python module path | /app |

### B. File Locations

| Component | Location |
|-----------|----------|
| Source Data | `data/raw/<source>/` |
| Staging Data | `data/staging/` |
| SQLMesh Models | `sqlMesh/models/` |
| Pipeline Code | `src/pipeline/` |
| Logs | `/var/log/osaa-pipeline/` |
| Config | `.env`, `docker-compose.yml` |

### C. Useful Commands

```bash
# View real-time logs
docker compose logs -f

# Execute SQL in DuckDB
docker compose run --rm pipeline duckdb_cli

# List all models
docker compose run --rm pipeline sqlmesh_list

# Validate model syntax
docker compose run --rm pipeline sqlmesh_validate

# Generate data lineage
docker compose run --rm pipeline sqlmesh_lineage

# Export data to CSV
docker compose run --rm pipeline export --format csv --output /tmp/export.csv
```

### D. SQL Snippets

```sql
-- Find missing data
SELECT country_id, year, COUNT(*) as missing_indicators
FROM (
    SELECT DISTINCT country_id, year FROM sources.sdg
) base
LEFT JOIN sources.sdg USING (country_id, year)
WHERE value IS NULL
GROUP BY country_id, year
HAVING COUNT(*) > 10;

-- Data profiling
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT country_id) as unique_countries,
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    AVG(value) as avg_value,
    STDDEV(value) as stddev_value
FROM master.indicators
WHERE source = 'sdg';

-- Identify anomalies
WITH stats AS (
    SELECT
        indicator_id,
        AVG(value) as mean_val,
        STDDEV(value) as std_val
    FROM sources.wdi
    GROUP BY indicator_id
)
SELECT
    w.*,
    ABS(w.value - s.mean_val) / NULLIF(s.std_val, 0) as z_score
FROM sources.wdi w
JOIN stats s ON w.indicator_id = s.indicator_id
WHERE ABS(w.value - s.mean_val) / NULLIF(s.std_val, 0) > 3;
```

## Contact Information

### Primary Contacts

| Role | Name | Email | Phone |
|------|------|-------|-------|
| Project Sponsor | Mirian Lima | mirian.lima@un.org | +1-XXX-XXX-XXXX |
| Principal Engineer | Stephen Sciortino | stephen.sciortino@un.org | +1-XXX-XXX-XXXX |
| Data Team Lead | TBD | data-team@un.org | +1-XXX-XXX-XXXX |
| DevOps Lead | TBD | devops@un.org | +1-XXX-XXX-XXXX |

### Escalation Matrix

| Severity | Response Time | Escalation Path |
|----------|---------------|-----------------|
| Critical | 15 minutes | Engineer → Team Lead → Director |
| High | 1 hour | Engineer → Team Lead |
| Medium | 4 hours | Engineer |
| Low | 24 hours | Engineer |

### Support Channels

- **GitHub Issues**: https://github.com/UN-OSAA/osaa-mvp/issues
- **Slack Channel**: #osaa-data-pipeline
- **Email List**: osaa-data-team@un.org
- **Documentation**: https://github.com/UN-OSAA/osaa-mvp/docs

### External Resources

- **SQLMesh Documentation**: https://sqlmesh.com/
- **DuckDB Documentation**: https://duckdb.org/docs/
- **AWS S3 Documentation**: https://docs.aws.amazon.com/s3/
- **Docker Documentation**: https://docs.docker.com/

---

*Last Updated: 2025-10-02*
*Version: 1.0.0*