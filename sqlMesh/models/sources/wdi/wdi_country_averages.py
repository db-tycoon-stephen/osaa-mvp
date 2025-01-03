from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
import ibis
import os
from macros.ibis_expressions import generate_ibis_table
from macros.s3_paths import s3_transformed_path
from models.sources.wdi.wdi_indicators import COLUMN_SCHEMA as WDI_COLUMN_SCHEMA

COLUMN_SCHEMA = {
    "country_id": "String",
    "indicator_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
    "avg_value_by_country": "Float",
}

# For post statement
SCHEMA_TO_COPY_FROM = (
    "" if os.getenv("TARGET") == "prod" else f"__{os.getenv('TARGET')}"
)


@model(
    "sources.wdi_country_averages",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    post_statements=[
        f"""
        @IF(
            @runtime_stage = 'testing',
                COPY (SELECT * FROM sources{SCHEMA_TO_COPY_FROM}.wdi_country_averages)
                TO {s3_transformed_path(MacroEvaluator, 'osaa_mvp.sources.wdi_country_averages')}
                (FORMAT PARQUET)
        );
    """
    ],
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    wdi = generate_ibis_table(
        evaluator,
        table_name="wdi",
        schema_name="sources",
        column_schema=WDI_COLUMN_SCHEMA,
    )

    country_averages = (
        wdi.filter(wdi.value.notnull())
        .group_by(["country_id", "indicator_id"])
        .agg(avg_value_by_country=wdi.value.mean())
        .join(wdi, ["country_id", "indicator_id"])
        .select(
            "country_id",
            "indicator_id",
            "year",
            "value",
            "magnitude",
            "qualifier",
            "indicator_description",
            "avg_value_by_country"
        )
    )

    return ibis.to_sql(country_averages)
