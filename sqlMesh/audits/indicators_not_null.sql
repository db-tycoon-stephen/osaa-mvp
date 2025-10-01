AUDIT (
    name indicators_not_null,
    description 'Verify that critical columns (indicator_id, country_id, year) are never null across all indicator datasets'
);

-- Check SDG data_national for null values in critical columns
SELECT
    'sdg.data_national' as source_table,
    COUNT(*) as null_count,
    'indicator_id' as column_name
FROM sdg.data_national
WHERE indicator_id IS NULL

UNION ALL

SELECT
    'sdg.data_national' as source_table,
    COUNT(*) as null_count,
    'country_id' as column_name
FROM sdg.data_national
WHERE country_id IS NULL

UNION ALL

SELECT
    'sdg.data_national' as source_table,
    COUNT(*) as null_count,
    'year' as column_name
FROM sdg.data_national
WHERE year IS NULL

UNION ALL

-- Check OPRI data_national for null values in critical columns
SELECT
    'opri.data_national' as source_table,
    COUNT(*) as null_count,
    'indicator_id' as column_name
FROM opri.data_national
WHERE indicator_id IS NULL

UNION ALL

SELECT
    'opri.data_national' as source_table,
    COUNT(*) as null_count,
    'country_id' as column_name
FROM opri.data_national
WHERE country_id IS NULL

UNION ALL

SELECT
    'opri.data_national' as source_table,
    COUNT(*) as null_count,
    'year' as column_name
FROM opri.data_national
WHERE year IS NULL

UNION ALL

-- Check WDI csv for null values in critical columns
SELECT
    'wdi.csv' as source_table,
    COUNT(*) as null_count,
    'Indicator Code' as column_name
FROM wdi.csv
WHERE "Indicator Code" IS NULL

UNION ALL

SELECT
    'wdi.csv' as source_table,
    COUNT(*) as null_count,
    'Country Code' as column_name
FROM wdi.csv
WHERE "Country Code" IS NULL

-- Only return rows where null_count > 0 (i.e., violations)
HAVING null_count > 0
ORDER BY source_table, column_name;
