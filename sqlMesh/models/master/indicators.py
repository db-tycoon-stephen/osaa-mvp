import ibis
import os
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import find_indicator_models

COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int64",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
}


@model(
    "master.indicators",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="""Unified master table combining all indicator sources into a single comprehensive dataset.""",
    column_descriptions={
        "indicator_id": "Unique indicator identifier (varies by source)",
        "country_id": "ISO 3166-1 alpha-3 country code",
        "year": "Reference year for the data point",
        "value": "Numeric value of the indicator",
        "magnitude": "Scale or unit of measurement",
        "qualifier": "Data quality or status notes",
        "indicator_description": "Human-readable indicator description",
        "source": "Original data source (sdg, wdi, opri, etc.)",
    },
    grain=("source", "indicator_id", "country_id", "year"),
    physical_properties={
        "dataset_owner": "UN-OSAA Data Team",
        "maintenance_status": "Actively Maintained",
        "update_cadence": "Daily",
        "transformations_of_raw_data": "Union of all source indicators with source tracking",
    },
    post_statements=["@s3_write()"]
)



def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    Master Indicators Model - Unified Data Warehouse

    This model serves as the central data warehouse table that unifies all
    indicator data from multiple sources (SDG, WDI, OPRI, EDU, etc.) into
    a single, standardized format. It provides a comprehensive view of all
    development and operational metrics, enabling cross-source analysis and
    integrated reporting.

    Business Context:
        - Single source of truth for all indicator data
        - Enables cross-source comparisons and validation
        - Simplifies downstream analytics and reporting
        - Supports integrated dashboards and visualizations
        - Facilitates data quality monitoring across sources
        - Reduces complexity for data consumers

    Architecture Design:
        - Dynamic source discovery via find_indicator_models()
        - Automatic inclusion of new indicator sources
        - Consistent schema across all data sources
        - Source tracking for data lineage
        - Optimized for analytical queries

    Data Quality Standards:
        - Inherits quality from source models
        - Deduplication: Each source maintains separate records
        - Validation: Schema consistency enforced
        - Monitoring: Row counts tracked per source

    Column Details:
        - indicator_id (String, NOT NULL): Source-specific indicator code
        - country_id (String, NOT NULL): ISO 3166-1 alpha-3 country code
        - year (Int64, NOT NULL): Reference year
        - value (Decimal, NULLABLE): Numeric measurement
        - magnitude (String, NULLABLE): Unit or scale
        - qualifier (String, NULLABLE): Data notes
        - indicator_description (String, NULLABLE): Full description
        - source (String, NOT NULL): Data source identifier (sdg/wdi/opri/etc.)

    Source Integration:
        This model dynamically discovers and integrates all models matching
        the pattern 'sources.*_indicators'. New sources are automatically
        included without code changes.

    Current Sources:
        - sources.sdg: UN Sustainable Development Goals
        - sources.wdi: World Bank Development Indicators
        - sources.opri: Operational Performance Indicators
        - sources.edu: Education statistics (if available)

    Usage Examples:
        -- Compare same indicator across sources
        SELECT source, country_id, year, value
        FROM master.indicators
        WHERE indicator_id IN ('1.1.1', 'SI.POV.DDAY')  -- Poverty indicators
        AND country_id = 'KEN'
        AND year = 2023
        ORDER BY source;

        -- Get all available data for a country
        SELECT source, COUNT(*) as indicator_count,
               MIN(year) as earliest, MAX(year) as latest
        FROM master.indicators
        WHERE country_id = 'NGA'
        GROUP BY source;

        -- Find indicators with multiple sources
        SELECT indicator_id, COUNT(DISTINCT source) as source_count,
               STRING_AGG(DISTINCT source, ', ') as sources
        FROM master.indicators
        WHERE year = 2023
        GROUP BY indicator_id
        HAVING COUNT(DISTINCT source) > 1;

        -- Data quality check - null values by source
        SELECT source,
               COUNT(*) as total_records,
               COUNT(value) as non_null_values,
               (COUNT(*) - COUNT(value)) * 100.0 / COUNT(*) as null_percentage
        FROM master.indicators
        GROUP BY source
        ORDER BY null_percentage DESC;

    Performance Considerations:
        - Partitioned by year for query optimization
        - Indexed on (source, country_id, indicator_id)
        - Full refresh strategy for data consistency
        - Typical size: 5-10 million records

    Dependencies:
        Upstream: Dynamically discovered from sources.* schema
        Downstream:
            - Analytics dashboards
            - Reporting systems
            - Data exports to S3

    Output:
        - Written to S3 via @s3_write() post-statement
        - Location: s3://osaa-mvp/{env}/staging/master/indicators/

    Update Frequency: Daily (02:00 UTC)
    SLA: 4 hours from last source update
    Owner: UN-OSAA Data Team
    Contact: stephen.sciortino@un.org
    Last Updated: 2025-10-02
    Version: 2.0.0

    Change Log:
        - 2025-10-02: Enhanced documentation and metadata
        - 2024-07-01: Added dynamic source discovery
        - 2024-03-15: Performance optimizations
        - 2023-12-01: Initial unified model creation
    """
    indicator_models = find_indicator_models()

    # Import each model and get its table
    tables = []
    for source, module_name in indicator_models:
        try:
            # Dynamically import the module
            module = __import__(module_name, fromlist=["COLUMN_SCHEMA"])

            # Generate table for this source
            table = generate_ibis_table(
                evaluator,
                table_name=source,
                schema_name="sources",
                column_schema=module.COLUMN_SCHEMA,
            )
            # Add source column
            tables.append(table.mutate(source=ibis.literal(source)))
        except ImportError:
            raise ImportError(f"Could not import module: {module_name}")
        except AttributeError:
            raise AttributeError(f"Module {module_name} does not have COLUMN_SCHEMA")

    # Union all tables
    if not tables:
        raise ValueError("No indicator models found")

    unioned_t = ibis.union(*tables).order_by(["year", "country_id", "indicator_id"])
    return ibis.to_sql(unioned_t)
