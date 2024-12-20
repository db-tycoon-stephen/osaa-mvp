import ibis 
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table


@model(
    "marts.indicators",
    is_sql=True,
    kind="FULL",
    columns={
        "indicator_id": "TEXT", 
        "country_id": "TEXT", 
        "year": "INT", 
        "value": "DECIMAL(18, 3)", 
        "magnitude": "TEXT", 
        "qualifier": "TEXT",
        "indicator_description": "TEXT", 
        "source": "TEXT"
    }
)
def entrypoint(evaluator: MacroEvaluator) -> str:

    column_schema = {
        "indicator_id": "String", 
        "country_id": "String", 
        "year": "Int64", 
        "value": "Decimal", 
        "magnitude": "String", 
        "qualifier": "String",
        "indicator_description": "String", 
    }

    int_sdg = generate_ibis_table(
        evaluator,
        table_name="sdg", 
        column_schema=column_schema, 
        schema_name="intermediate"
    )

    int_opri = generate_ibis_table(
        evaluator,
        table_name="opri", 
        column_schema=column_schema, 
        schema_name="intermediate"
    )
   
    unioned_t = (
        ibis
        .union(
            int_sdg.mutate(source=ibis.literal("sdg")),
            int_opri.mutate(source=ibis.literal("opri"))
        )
        .order_by([
            'year',
            'country_id',
            'indicator_id'
        ])
    )

    return ibis.to_sql(unioned_t)