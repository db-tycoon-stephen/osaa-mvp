AUDIT (
    name indicators_unique_grain,
    description 'Verify that the grain (indicator_id, country_id, year) is unique across all indicator datasets to prevent duplicate records'
);

-- Check SDG data_national for duplicate grain combinations
SELECT
    'sdg.data_national' as source_table,
    indicator_id,
    country_id,
    year,
    COUNT(*) as duplicate_count
FROM sdg.data_national
GROUP BY indicator_id, country_id, year
HAVING COUNT(*) > 1

UNION ALL

-- Check OPRI data_national for duplicate grain combinations
SELECT
    'opri.data_national' as source_table,
    indicator_id,
    country_id,
    year,
    COUNT(*) as duplicate_count
FROM opri.data_national
GROUP BY indicator_id, country_id, year
HAVING COUNT(*) > 1

UNION ALL

-- Check WDI csv for duplicate grain combinations
SELECT
    'wdi.csv' as source_table,
    "Indicator Code" as indicator_id,
    "Country Code" as country_id,
    NULL as year,  -- WDI has wide format with year columns
    COUNT(*) as duplicate_count
FROM wdi.csv
GROUP BY "Indicator Code", "Country Code"
HAVING COUNT(*) > 1

ORDER BY source_table, indicator_id, country_id, year;
