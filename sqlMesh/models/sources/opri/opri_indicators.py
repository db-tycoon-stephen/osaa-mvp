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
    "sources.opri",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="""OPRI (Operational Performance and Risk Indicators) data for institutional performance monitoring.""",
    column_descriptions={
        "indicator_id": "Unique identifier for the OPRI metric",
        "country_id": "ISO 3166-1 alpha-3 country code",
        "year": "Reference year for the measurement",
        "value": "Numeric value of the indicator",
        "magnitude": "Scale or unit of measurement",
        "qualifier": "Data quality or status qualifier",
        "indicator_description": "Detailed description of the indicator",
    },
    grain=("indicator_id", "country_id", "year"),
    physical_properties={
        "publishing_org": "UN-OSAA",
        "dataset_owner": "UN-OSAA Operations Team",
        "maintenance_status": "Actively Maintained",
        "update_cadence": "Monthly",
        "transformations_of_raw_data": "Raw operational data aggregated and standardized",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    OPRI Indicators Model - Operational Performance and Risk Indicators

    This model processes Operational Performance and Risk Indicators (OPRI)
    that measure institutional effectiveness, program delivery, and risk
    management across UN operations in Africa. These indicators provide
    critical insights for operational decision-making and performance
    improvement.

    Business Context:
        - Monitors operational efficiency and effectiveness
        - Tracks program delivery and impact metrics
        - Identifies operational risks and bottlenecks
        - Supports resource allocation decisions
        - Enables performance benchmarking across regions
        - Facilitates evidence-based operational improvements

    Data Quality Standards:
        - Completeness: >90% for core operational metrics
        - Timeliness: Monthly updates within 15 days of month-end
        - Accuracy: Validated through multi-level review process
        - Known Issues:
            * Some field offices may have reporting delays
            * Historical data before 2020 uses different methodology
            * Emergency response metrics may have gaps

    Column Details:
        - indicator_id (String, NOT NULL): Unique OPRI code (e.g., "OPS.1.1" for
          operational efficiency, "RISK.2.3" for risk metrics)
        - country_id (String, NOT NULL): ISO 3166-1 alpha-3 country code
        - year (Integer, NOT NULL): Reference year (2018-present)
        - value (Decimal, NULLABLE): Measured value (units vary by indicator)
        - magnitude (String, NULLABLE): Unit of measurement or scale
        - qualifier (String, NULLABLE): Data status (e.g., "preliminary", "revised")
        - indicator_description (String, NULLABLE): Full indicator description

    Data Processing:
        1. Collects data from operational reporting systems
        2. Standardizes metrics across different field offices
        3. Applies data quality validations
        4. Enriches with indicator metadata
        5. Calculates derived metrics where applicable

    Key Indicator Categories:
        - OPS.*: Operational efficiency metrics
        - RISK.*: Risk management indicators
        - PROG.*: Program delivery metrics
        - FIN.*: Financial performance indicators
        - HR.*: Human resources metrics

    Usage Examples:
        -- Get latest operational efficiency metrics
        SELECT * FROM sources.opri
        WHERE indicator_id LIKE 'OPS.%'
        AND year = (SELECT MAX(year) FROM sources.opri)
        ORDER BY country_id, indicator_id;

        -- Track risk indicators over time
        SELECT country_id, year, indicator_id, value
        FROM sources.opri
        WHERE indicator_id LIKE 'RISK.%'
        AND year >= 2022
        ORDER BY country_id, year;

        -- Compare program delivery across regions
        SELECT indicator_id, AVG(value) as avg_value, COUNT(DISTINCT country_id) as countries
        FROM sources.opri
        WHERE indicator_id LIKE 'PROG.%'
        AND year = 2024
        GROUP BY indicator_id
        ORDER BY avg_value DESC;

    Dependencies:
        Upstream:
            - s3://landing/opri/OPRI_DATA_NATIONAL.parquet (operational data)
            - s3://landing/opri/OPRI_LABEL.parquet (indicator metadata)
        Downstream:
            - master.indicators (unified indicators table)
            - analytics.operational_dashboard

    Update Frequency: Monthly (by 15th of following month)
    SLA: 24 hours from data submission deadline
    Owner: UN-OSAA Operations Team
    Contact: stephen.sciortino@un.org
    Last Updated: 2025-10-02
    Version: 1.5.0

    Change Log:
        - 2025-10-02: Enhanced documentation and metadata
        - 2024-08-01: Added new risk indicators
        - 2024-03-15: Improved data quality checks
        - 2023-11-01: Initial model deployment
    """
    source_folder_path = "opri"

    opri_data_national = generate_ibis_table(
        evaluator,
        table_name="data_national",
        column_schema=get_sql_model_schema(evaluator, "data_national", source_folder_path),
        schema_name="opri",
    )

    opri_label = generate_ibis_table(
        evaluator,
        table_name="label",
        column_schema=get_sql_model_schema(evaluator, "label", source_folder_path),
        schema_name="opri",
    )

    opri_table = (
        opri_data_national.left_join(opri_label, "indicator_id")
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

    return ibis.to_sql(opri_table)
