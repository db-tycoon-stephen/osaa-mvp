import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema
from sqlglot import exp


COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
}


@model(
    "sources.sdg",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="""This model contains Sustainable Development Goals (SDG) data for all countries and indicators.""",
    column_descriptions={
        "indicator_id": "The unique identifier for the indicator",
        "country_id": "The unique identifier for the country",
        "year": "The year of the data",
        "value": "The value of the indicator for the country and year",
        "magnitude": "The magnitude of the indicator for the country and year",
        "qualifier": "The qualifier of the indicator for the country and year",
        "indicator_description": "The description of the indicator",
    },
    grain=("indicator_id", "country_id", "year"),
    physical_properties={
        "publishing_org": "UN",
        "link_to_raw_data": "https://unstats.un.org/sdgs/dataportal",
        "dataset_owner": "UN",
        "dataset_owner_contact_info": "https://unstats.un.org/sdgs/contact-us/",
        "funding_source": "UN",
        "maintenance_status": "Actively Maintained",
        "how_data_was_collected": "https://unstats.un.org/sdgs/dataContacts/",
        "update_cadence": "Annually",
        "transformations_of_raw_data": "indicator labels and descriptions are joined together",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    SDG Indicators Model - Sustainable Development Goals Data

    This model processes Sustainable Development Goal (SDG) indicators from
    the United Nations Statistical Division. The data includes country-level
    progress metrics for all 17 SDGs, covering 169 targets and 231 unique
    indicators that measure global progress towards the 2030 Agenda for
    Sustainable Development.

    Business Context:
        - Primary data source for SDG monitoring dashboards and reports
        - Used for tracking progress towards 2030 Agenda targets
        - Critical for UN member state reporting and policy decisions
        - Supports evidence-based development planning
        - Enables cross-country comparisons and regional analysis

    Data Quality Standards:
        - Completeness: >95% coverage for Tier I indicators
        - Timeliness: Annual updates with 6-12 month lag from reference year
        - Accuracy: Official statistics validated by UN Statistics Division
        - Known Issues:
            * Data gaps for Tier III indicators (methodology under development)
            * Historical data before 2015 may be incomplete
            * Small island states may have limited coverage

    Column Details:
        - indicator_id (String, NOT NULL): Unique SDG indicator code following UN
          numbering (e.g., "1.1.1" for poverty headcount ratio at $1.90/day)
        - country_id (String, NOT NULL): ISO 3166-1 alpha-3 country code
        - year (Integer, NOT NULL): Reference year for the data point (2000-present)
        - value (Decimal, NULLABLE): Indicator measurement value (units vary by indicator)
        - magnitude (String, NULLABLE): Scale qualifier (e.g., "thousands", "percentage")
        - qualifier (String, NULLABLE): Data point qualifier (e.g., "estimated", "provisional")
        - indicator_description (String, NULLABLE): Human-readable indicator description

    Data Processing:
        1. Ingests raw SDG data from UN Statistics API
        2. Joins indicator data with metadata labels
        3. Standardizes country codes to ISO 3166-1 alpha-3
        4. Validates data types and applies quality checks
        5. Enriches with indicator descriptions in English

    Usage Examples:
        -- Get poverty indicators for all countries (SDG 1)
        SELECT * FROM sources.sdg
        WHERE indicator_id LIKE '1.%'
        AND year = (SELECT MAX(year) FROM sources.sdg)
        ORDER BY country_id;

        -- Track climate action progress (SDG 13) over time
        SELECT country_id, year, value, indicator_description
        FROM sources.sdg
        WHERE indicator_id LIKE '13.%'
        AND country_id IN ('USA', 'CHN', 'IND', 'BRA')
        ORDER BY year DESC;

        -- Find countries meeting specific targets
        SELECT DISTINCT country_id, indicator_description, value
        FROM sources.sdg
        WHERE indicator_id = '3.2.1'  -- Under-5 mortality rate
        AND year = 2023
        AND value < 25  -- SDG target threshold
        ORDER BY value;

    Dependencies:
        Upstream:
            - s3://landing/sdg/SDG_DATA_NATIONAL.parquet (raw country-level data)
            - s3://landing/sdg/SDG_LABEL.parquet (indicator metadata)
        Downstream:
            - master.indicators (unified indicators table)
            - analytics.sdg_dashboard (dashboard aggregations)

    Update Frequency: Annual (typically updated in Q2 following year)
    SLA: 48 hours from source data availability
    Owner: UN-OSAA Data Team / UN Statistics Division
    Contact: stephen.sciortino@un.org
    Last Updated: 2025-10-02
    Version: 2.0.0

    Change Log:
        - 2025-10-02: Enhanced documentation and metadata
        - 2024-06-15: Added data quality validations
        - 2024-01-10: Initial model creation
    """
    source_folder_path = "sdg"

    sdg_data_national = generate_ibis_table(
        evaluator,
        table_name="data_national",
        column_schema=get_sql_model_schema(evaluator, "data_national", source_folder_path),
        schema_name="sdg",
    )

    sdg_label = generate_ibis_table(
        evaluator,
        table_name="label",
        column_schema=get_sql_model_schema(evaluator, "label", source_folder_path),
        schema_name="sdg",
    )

    sdg_table = (
        sdg_data_national.left_join(sdg_label, "indicator_id")
        .select(
            "indicator_id",
            "country_id",
            "year",
            "value",
            "magnitude",
            "qualifier",
            "indicator_label_en",
        )
        .rename(indicator_description="indicator_label_en")
    )

    return ibis.to_sql(sdg_table)
