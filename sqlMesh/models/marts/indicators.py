import ibis 
from ibis.expr.operations import Namespace, UnboundTable
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model


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

    schema = {
        "indicator_id": "String", 
        "country_id": "String", 
        "year": "Int64", 
        "value": "Decimal", 
        "magnitude": "String", 
        "qualifier": "String",
        "indicator_description": "String", 
    }

    int_sdg = UnboundTable(
        name="sdg",
        schema=schema,
        namespace=Namespace(database="intermediate")
    ).to_expr()

    int_opri = UnboundTable(
        name="opri",
        schema=schema,
        namespace=Namespace(database="intermediate")
    ).to_expr()
   
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