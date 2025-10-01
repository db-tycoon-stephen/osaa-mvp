AUDIT (
    name indicators_value_ranges,
    description 'Verify that year and value columns are within reasonable ranges for data quality'
);

-- Check SDG data_national for out-of-range years
SELECT
    'sdg.data_national' as source_table,
    'year_out_of_range' as violation_type,
    indicator_id,
    country_id,
    year,
    value,
    CASE
        WHEN year < 1960 THEN 'Year before 1960'
        WHEN year > 2030 THEN 'Year after 2030'
    END as issue_description
FROM sdg.data_national
WHERE year < 1960 OR year > 2030

UNION ALL

-- Check OPRI data_national for out-of-range years
SELECT
    'opri.data_national' as source_table,
    'year_out_of_range' as violation_type,
    indicator_id,
    country_id,
    year,
    value,
    CASE
        WHEN year < 1960 THEN 'Year before 1960'
        WHEN year > 2030 THEN 'Year after 2030'
    END as issue_description
FROM opri.data_national
WHERE year < 1960 OR year > 2030

UNION ALL

-- Check for extreme outlier values (more than 6 orders of magnitude)
SELECT
    'sdg.data_national' as source_table,
    'extreme_value' as violation_type,
    indicator_id,
    country_id,
    year,
    value,
    'Value exceeds reasonable range (abs > 1e12)' as issue_description
FROM sdg.data_national
WHERE ABS(value) > 1e12

UNION ALL

SELECT
    'opri.data_national' as source_table,
    'extreme_value' as violation_type,
    indicator_id,
    country_id,
    year,
    value,
    'Value exceeds reasonable range (abs > 1e12)' as issue_description
FROM opri.data_national
WHERE ABS(value) > 1e12

ORDER BY source_table, violation_type, year DESC;
