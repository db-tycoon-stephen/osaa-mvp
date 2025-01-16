from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
import ibis
from macros.ibis_expressions import generate_ibis_table
from sqlglot import exp
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


@model(
    "sources.wdi_country_averages",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    post_statements=["@s3_write()"]
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
