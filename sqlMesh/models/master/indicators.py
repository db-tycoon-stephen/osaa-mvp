import ibis
import os
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from constants import SQLMESH_DIR

COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int64",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
}


def find_indicator_models():
    """Find all models ending with _indicators in the sources directory."""
    indicator_models = []
    sources_dir = os.path.join(SQLMESH_DIR, "models", "sources")
    
    for source in os.listdir(sources_dir):
        source_dir = os.path.join(sources_dir, source)
        if os.path.isdir(source_dir):
            for file in os.listdir(source_dir):
                if file.endswith('_indicators.py'):
                    module_name = f"models.sources.{source}.{file[:-3]}"
                    indicator_models.append((source, module_name))
    
    return indicator_models


@model(
    "master.indicators",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    # Find all indicator models
    indicator_models = find_indicator_models()
    
    # Import each model and get its table
    tables = []
    for source, module_name in indicator_models:
        # Dynamically import the module
        module = __import__(module_name, fromlist=['COLUMN_SCHEMA'])
        
        # Generate table for this source
        table = generate_ibis_table(
            evaluator,
            table_name=source,
            schema_name="sources",
            column_schema=module.COLUMN_SCHEMA,
        )
        # Add source column
        tables.append(table.mutate(source=ibis.literal(source)))
    
    # Union all tables
    if not tables:
        raise ValueError("No indicator models found")
    
    unioned_t = ibis.union(*tables).order_by(["year", "country_id", "indicator_id"])
    return ibis.to_sql(unioned_t)
