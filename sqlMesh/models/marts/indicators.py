import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from models.intermediate.sdg import column_schema as sdg_column_schema
from models.intermediate.opri import column_schema as opri_column_schema


@model(
    "marts.indicators",
    is_sql=True,
    kind="FULL",
    columns={
        "indicator_id": "String",
        "country_id": "String",
        "year": "Int64",
        "value": "Decimal",
        "magnitude": "String",
        "qualifier": "String",
        "indicator_description": "String",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    int_sdg = generate_ibis_table(
        evaluator,
        table_name="sdg",
        schema_name="intermediate",
        column_schema=sdg_column_schema,
    )

    int_opri = generate_ibis_table(
        evaluator,
        table_name="opri",
        schema_name="intermediate",
        column_schema=opri_column_schema,
    )

    unioned_t = ibis.union(
        int_sdg.mutate(source=ibis.literal("sdg")),
        int_opri.mutate(source=ibis.literal("opri")),
    ).order_by(["year", "country_id", "indicator_id"])

    return ibis.to_sql(unioned_t)
