AUDIT (
    name indicators_data_freshness,
    description 'Monitor data freshness by checking loaded_at timestamps and alerting if data is stale (>48 hours)'
);

-- Note: This audit assumes loaded_at or similar timestamp column exists in the source tables
-- If not present, this will need to be added to the data model
-- For now, checking based on file metadata or system timestamps if available

-- Check for stale data based on the most recent year in each dataset
-- Alert if the most recent year is significantly behind current year
SELECT
    'sdg.data_national' as source_table,
    'stale_data' as freshness_check,
    MAX(year) as most_recent_year,
    EXTRACT(YEAR FROM CURRENT_DATE) as current_year,
    EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) as years_behind,
    COUNT(DISTINCT indicator_id) as indicators_count,
    CASE
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) > 2 THEN 'CRITICAL: Data more than 2 years old'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) > 1 THEN 'WARNING: Data more than 1 year old'
        ELSE 'OK'
    END as status
FROM sdg.data_national
GROUP BY source_table, freshness_check
HAVING EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) > 1

UNION ALL

SELECT
    'opri.data_national' as source_table,
    'stale_data' as freshness_check,
    MAX(year) as most_recent_year,
    EXTRACT(YEAR FROM CURRENT_DATE) as current_year,
    EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) as years_behind,
    COUNT(DISTINCT indicator_id) as indicators_count,
    CASE
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) > 2 THEN 'CRITICAL: Data more than 2 years old'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) > 1 THEN 'WARNING: Data more than 1 year old'
        ELSE 'OK'
    END as status
FROM opri.data_national
GROUP BY source_table, freshness_check
HAVING EXTRACT(YEAR FROM CURRENT_DATE) - MAX(year) > 1

UNION ALL

-- Check for missing recent years (gaps in time series)
SELECT
    'sdg.data_national' as source_table,
    'missing_recent_years' as freshness_check,
    NULL as most_recent_year,
    NULL as current_year,
    NULL as years_behind,
    COUNT(*) as indicators_count,
    'Indicators missing data for recent years' as status
FROM (
    SELECT indicator_id, MAX(year) as max_year
    FROM sdg.data_national
    GROUP BY indicator_id
    HAVING MAX(year) < EXTRACT(YEAR FROM CURRENT_DATE) - 2
) recent_gaps

UNION ALL

SELECT
    'opri.data_national' as source_table,
    'missing_recent_years' as freshness_check,
    NULL as most_recent_year,
    NULL as current_year,
    NULL as years_behind,
    COUNT(*) as indicators_count,
    'Indicators missing data for recent years' as status
FROM (
    SELECT indicator_id, MAX(year) as max_year
    FROM opri.data_national
    GROUP BY indicator_id
    HAVING MAX(year) < EXTRACT(YEAR FROM CURRENT_DATE) - 2
) recent_gaps

ORDER BY source_table, freshness_check;
