import ibis 
from sqlmesh import ExecutionContext, model
import typing as t
from datetime import datetime


@model(
    "marts.indicators",
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
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> str:
    
    int_sdg = context.table("intermediate.sdg")
    int_opri = context.table("intermediate.opri")
    int_sdg_t = ibis.memtable(context.fetchdf(f"SELECT * FROM {int_sdg}"))
    int_opri_t = ibis.memtable(context.fetchdf(f"SELECT * FROM {int_opri}"))
   
    unioned_t = (
        ibis
        .union(
            int_sdg_t.mutate(source=ibis.literal("sdg")),
            int_opri_t.mutate(source=ibis.literal("opri"))
        )
        .order_by([
            'year',
            'country_id',
            'indicator_id'
        ])
    ) 

    return unioned_t.to_pandas()