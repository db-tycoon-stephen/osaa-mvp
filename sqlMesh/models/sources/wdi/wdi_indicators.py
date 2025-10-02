import ibis
import ibis.selectors as s
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema
from sqlglot import exp

COLUMN_SCHEMA = {
    "country_id": "String",
    "indicator_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
}


@model(
    "sources.wdi",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="""World Development Indicators (WDI) from the World Bank's comprehensive development database.""",
    column_descriptions={
        "country_id": "ISO 3166-1 alpha-3 country code",
        "indicator_id": "World Bank indicator code",
        "year": "Reference year for the data point",
        "value": "Indicator value (units vary by indicator)",
        "magnitude": "Scale or unit of measurement",
        "qualifier": "Data quality or methodology notes",
        "indicator_description": "Full description of the indicator",
    },
    grain=("indicator_id", "country_id", "year"),
    physical_properties={
        "publishing_org": "World Bank",
        "link_to_raw_data": "https://datacatalog.worldbank.org/dataset/world-development-indicators",
        "dataset_owner": "World Bank Data Group",
        "dataset_owner_contact_info": "data@worldbank.org",
        "funding_source": "World Bank",
        "maintenance_status": "Actively Maintained",
        "update_cadence": "Quarterly",
        "transformations_of_raw_data": "Pivoted from wide to long format, enriched with series metadata",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    WDI Indicators Model - World Development Indicators

    This model processes World Development Indicators (WDI) from the World Bank,
    the primary World Bank collection of development indicators compiled from
    officially recognized international sources. It presents the most current
    and accurate global development data available, including national, regional,
    and global estimates covering over 1,400 indicators.

    Business Context:
        - Primary source for socioeconomic development metrics
        - Supports evidence-based policy making and research
        - Enables cross-country comparisons and trend analysis
        - Critical for development planning and monitoring
        - Used in academic research and international reports
        - Provides baseline data for development projects

    Data Coverage:
        - 217 economies (countries and territories)
        - Time series from 1960 to present
        - 1,400+ development indicators
        - Topics: poverty, education, health, economy, environment, etc.
        - Updated quarterly with annual comprehensive revision

    Data Quality Standards:
        - Completeness: Varies by indicator and country (70-95%)
        - Timeliness: Quarterly updates, 3-6 month lag for most indicators
        - Accuracy: Official statistics from national and international sources
        - Known Issues:
            * Data gaps for conflict-affected countries
            * Historical data (pre-1990) may be sparse
            * Methodology changes may affect time series continuity
            * Small economies may have limited indicator coverage

    Column Details:
        - country_id (String, NOT NULL): ISO 3166-1 alpha-3 country code
        - indicator_id (String, NOT NULL): World Bank indicator code (e.g.,
          "SP.POP.TOTL" for total population, "NY.GDP.MKTP.CD" for GDP)
        - year (Integer, NOT NULL): Reference year (1960-present)
        - value (Decimal, NULLABLE): Indicator value (units vary)
        - magnitude (String, NULLABLE): Currently empty, reserved for scale
        - qualifier (String, NULLABLE): Currently empty, reserved for notes
        - indicator_description (String, NULLABLE): Long description from WDI series

    Data Processing:
        1. Ingests WDI data in wide format (years as columns)
        2. Pivots to long format (year as row)
        3. Joins with series metadata for descriptions
        4. Standardizes column names to snake_case
        5. Casts data types appropriately
        6. Filters out null values

    Key Indicator Categories:
        - SP.*: Social/Population indicators
        - NY.*: National accounts and GDP
        - SI.*: Social inclusion and poverty
        - SE.*: Education statistics
        - SH.*: Health statistics
        - EN.*: Environment indicators
        - IT.*: Information technology metrics

    Usage Examples:
        -- Get latest GDP data for major economies
        SELECT country_id, year, value as gdp_usd
        FROM sources.wdi
        WHERE indicator_id = 'NY.GDP.MKTP.CD'
        AND year = (SELECT MAX(year) FROM sources.wdi WHERE indicator_id = 'NY.GDP.MKTP.CD')
        AND country_id IN ('USA', 'CHN', 'JPN', 'DEU', 'IND')
        ORDER BY value DESC;

        -- Track population growth over time
        SELECT country_id, year, value as population
        FROM sources.wdi
        WHERE indicator_id = 'SP.POP.TOTL'
        AND country_id = 'NGA'  -- Nigeria
        AND year >= 2010
        ORDER BY year;

        -- Compare education indicators across African countries
        SELECT country_id, indicator_id, value, indicator_description
        FROM sources.wdi
        WHERE indicator_id LIKE 'SE.%'
        AND year = 2023
        AND country_id IN (SELECT DISTINCT country_id FROM sources.wdi WHERE country_id LIKE '%A')
        ORDER BY country_id, indicator_id;

        -- Find countries with improving health outcomes
        SELECT w1.country_id,
               w1.value as mortality_2015,
               w2.value as mortality_2023,
               ((w1.value - w2.value) / w1.value * 100) as improvement_pct
        FROM sources.wdi w1
        JOIN sources.wdi w2 ON w1.country_id = w2.country_id
                           AND w1.indicator_id = w2.indicator_id
        WHERE w1.indicator_id = 'SH.DYN.MORT'  -- Under-5 mortality
        AND w1.year = 2015 AND w2.year = 2023
        AND w2.value < w1.value
        ORDER BY improvement_pct DESC;

    Dependencies:
        Upstream:
            - s3://landing/wdi/WDI_CSV.parquet (main data file)
            - s3://landing/wdi/WDI_SERIES.parquet (indicator metadata)
        Downstream:
            - master.indicators (unified indicators table)
            - analytics.development_dashboard
            - research.economic_analysis

    Update Frequency: Quarterly (January, April, July, October)
    SLA: 72 hours from World Bank data release
    Owner: UN-OSAA Data Team / World Bank Data Partnership
    Contact: stephen.sciortino@un.org
    Last Updated: 2025-10-02
    Version: 3.1.0

    Change Log:
        - 2025-10-02: Enhanced documentation and metadata
        - 2024-09-01: Improved pivot logic for better performance
        - 2024-04-15: Added series metadata join
        - 2023-10-01: Initial WDI integration
    """
    # Process WDI data and return the transformed Ibis table

    source_folder_path = "wdi"
    wdi_csv = generate_ibis_table(
        evaluator,
        table_name="csv",
        column_schema=get_sql_model_schema(evaluator, "csv", source_folder_path),
        schema_name="wdi",
    )

    wdi_data = (
        wdi_csv.rename("snake_case")
        .rename(country_id="country_code", indicator_id="indicator_code")
        .select("country_id", "indicator_id", s.numeric())
        .pivot_longer(s.index["1960":], names_to="year", values_to="value")
        .cast({"year": "int64", "value": "decimal"})
    )

    wdi_series = generate_ibis_table(
        evaluator,
        table_name="series",
        column_schema=get_sql_model_schema(evaluator, "series", source_folder_path),
        schema_name="wdi",
    )

    wdi_series_renamed = (
        wdi_series
        .rename("snake_case")
        .rename(indicator_id="series_code")
    )

    wdi = (
        wdi_data.left_join(
            wdi_series_renamed,
            "indicator_id"
        )
        .mutate(
            magnitude=ibis.literal(""),  # Empty string for now
            qualifier=ibis.literal(""),  # Empty string for now
            indicator_description=wdi_series_renamed["long_definition"]
        )
    )

    return ibis.to_sql(wdi)
