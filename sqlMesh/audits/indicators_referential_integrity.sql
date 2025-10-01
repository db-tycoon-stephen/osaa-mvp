AUDIT (
    name indicators_referential_integrity,
    description 'Verify referential integrity between data tables and their corresponding label/metadata tables'
);

-- Check SDG data_national for indicator_ids that don't have corresponding labels
SELECT
    'sdg.data_national' as source_table,
    'missing_indicator_label' as violation_type,
    d.indicator_id,
    COUNT(DISTINCT d.country_id) as affected_countries,
    COUNT(DISTINCT d.year) as affected_years,
    COUNT(*) as affected_records
FROM sdg.data_national d
LEFT JOIN sdg.label l ON d.indicator_id = l.indicator_id
WHERE l.indicator_id IS NULL
GROUP BY d.indicator_id

UNION ALL

-- Check OPRI data_national for indicator_ids that don't have corresponding labels
SELECT
    'opri.data_national' as source_table,
    'missing_indicator_label' as violation_type,
    d.indicator_id,
    COUNT(DISTINCT d.country_id) as affected_countries,
    COUNT(DISTINCT d.year) as affected_years,
    COUNT(*) as affected_records
FROM opri.data_national d
LEFT JOIN opri.label l ON d.indicator_id = l.indicator_id
WHERE l.indicator_id IS NULL
GROUP BY d.indicator_id

UNION ALL

-- Check WDI csv for indicator codes that don't have corresponding series metadata
SELECT
    'wdi.csv' as source_table,
    'missing_series_metadata' as violation_type,
    w."Indicator Code" as indicator_id,
    COUNT(DISTINCT w."Country Code") as affected_countries,
    0 as affected_years,  -- WDI has wide format
    COUNT(*) as affected_records
FROM wdi.csv w
LEFT JOIN wdi.series s ON w."Indicator Code" = s."Series Code"
WHERE s."Series Code" IS NULL
GROUP BY w."Indicator Code"

ORDER BY source_table, affected_records DESC;
