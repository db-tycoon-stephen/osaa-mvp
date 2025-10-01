AUDIT (
    name indicators_completeness,
    description 'Check expected country coverage and monitor indicator availability across datasets'
);

-- Check for indicators with low country coverage (less than 10 countries)
SELECT
    'sdg.data_national' as source_table,
    'low_country_coverage' as completeness_check,
    indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    'Indicator has fewer than 10 countries' as issue
FROM sdg.data_national
GROUP BY indicator_id
HAVING COUNT(DISTINCT country_id) < 10

UNION ALL

SELECT
    'opri.data_national' as source_table,
    'low_country_coverage' as completeness_check,
    indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    'Indicator has fewer than 10 countries' as issue
FROM opri.data_national
GROUP BY indicator_id
HAVING COUNT(DISTINCT country_id) < 10

UNION ALL

-- Check for indicators with sparse time series (less than 5 years)
SELECT
    'sdg.data_national' as source_table,
    'sparse_time_series' as completeness_check,
    indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    'Indicator has fewer than 5 years of data' as issue
FROM sdg.data_national
GROUP BY indicator_id
HAVING COUNT(DISTINCT year) < 5

UNION ALL

SELECT
    'opri.data_national' as source_table,
    'sparse_time_series' as completeness_check,
    indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    'Indicator has fewer than 5 years of data' as issue
FROM opri.data_national
GROUP BY indicator_id
HAVING COUNT(DISTINCT year) < 5

UNION ALL

-- Check for countries that are completely missing from datasets
SELECT
    'sdg.data_national' as source_table,
    'dataset_coverage' as completeness_check,
    NULL as indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    'Total countries in SDG dataset' as issue
FROM sdg.data_national

UNION ALL

SELECT
    'opri.data_national' as source_table,
    'dataset_coverage' as completeness_check,
    NULL as indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    'Total countries in OPRI dataset' as issue
FROM opri.data_national

UNION ALL

SELECT
    'wdi.csv' as source_table,
    'dataset_coverage' as completeness_check,
    NULL as indicator_id,
    COUNT(DISTINCT "Country Code") as country_count,
    0 as year_count,
    COUNT(*) as total_records,
    'Total countries in WDI dataset' as issue
FROM wdi.csv

UNION ALL

-- Check for null value rates (high percentage of null values indicates poor data quality)
SELECT
    'sdg.data_national' as source_table,
    'high_null_rate' as completeness_check,
    indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    CONCAT('Null value rate: ', ROUND(100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%') as issue
FROM sdg.data_national
GROUP BY indicator_id
HAVING 100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) > 50

UNION ALL

SELECT
    'opri.data_national' as source_table,
    'high_null_rate' as completeness_check,
    indicator_id,
    COUNT(DISTINCT country_id) as country_count,
    COUNT(DISTINCT year) as year_count,
    COUNT(*) as total_records,
    CONCAT('Null value rate: ', ROUND(100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%') as issue
FROM opri.data_national
GROUP BY indicator_id
HAVING 100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) > 50

ORDER BY source_table, completeness_check, country_count;
