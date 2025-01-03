import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from models.sources.sdg import COLUMN_SCHEMA as SDG_COLUMN_SCHEMA
from models.sources.opri import COLUMN_SCHEMA as OPRI_COLUMN_SCHEMA

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
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    sdg = generate_ibis_table(
        evaluator,
        table_name="sdg",
        schema_name="sources",
        column_schema=SDG_COLUMN_SCHEMA,
    )

    opri = generate_ibis_table(
        evaluator,
        table_name="opri",
        schema_name="sources",
        column_schema=OPRI_COLUMN_SCHEMA,
    )

    unioned_t = ibis.union(
        sdg.mutate(source=ibis.literal("sdg")),
        opri.mutate(source=ibis.literal("opri")),
    ).order_by(["year", "country_id", "indicator_id"])

    return ibis.to_sql(unioned_t)
