MODEL (
  name intermediate.sdg,
  kind FULL,
  description 'This model contains Sustainable Development Goals (SDG) data for all countries and indicators.
  Publishing Org: UN
  Link to raw data: https://unstats.un.org/sdgs/dataportal
  Dataset owner:
    Name: UN
    Contact info: https://unstats.un.org/sdgs/contact-us/
  Funding Source: 
  Maintenance Status: Actively Maintained
  How data was collected: https://unstats.un.org/sdgs/dataContacts/
  Update Cadence: Annually
  Transformations of raw data (column renaming, type casting, etc): indicator labels and descriptions are joined together
  Column descriptions:
    indicator_id: The unique identifier for the indicator
    country_id: The unique identifier for the country
    year: The year of the data
    value: The value of the indicator for the country and year
    magnitude: The magnitude of the indicator for the country and year
    qualifier: The qualifier of the indicator for the country and year
    indicator_description: The description of the indicator
  Primary Key: indicator_id, country_id, year
    '
);

SELECT
  series.indicator_id::TEXT,
  series.country_id::TEXT,
  series.year::INT,
  series.value::DECIMAL(18, 3),
  series.magnitude::TEXT,
  series.qualifier::TEXT,
  label.indicator_label_en::TEXT AS indicator_description
FROM sdg.data_national AS series
LEFT JOIN sdg.label AS label
  ON series.indicator_id = label.indicator_id;
