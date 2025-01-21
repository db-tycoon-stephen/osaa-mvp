import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from constants import DB_PATH


COLUMN_SCHEMA = {
    "model_name": "String",
    "model_description": "String",
    "model_kind": "String",
    "grain": "String",
    "columns": "String",
    "column_descriptions": "String",
    "physical_properties": "String",
}


@model(
    "_metadata.all_models",
    is_sql=True,
    kind="FULL",
    depends_on=["master.indicators"],
    columns=COLUMN_SCHEMA,
    post_statements=["@s3_write()"],
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    This model is used to get the model properties of the latest snapshot.
    The user may see the following warning the first time they run the project, but they can safely ignore it:
    ```
    2025-01-21 20:39:23,269 - MainThread - sqlmesh.core.renderer - WARNING - SELECT * cannot be expanded due to missing schema(s) for model(s): '"unbound_table_1"'.
    Run `sqlmesh create_external_models` and / or make sure that the model '"osaa_mvp"."_metadata"."all_models"' can be rendered at parse time. (renderer.py:540)
    ```
    """
    con = ibis.connect(f"duckdb://{DB_PATH}")

    try:
        query = """
            SELECT
                json_extract(s.snapshot, '$.node.name') AS model_name,
                json_extract(s.snapshot, '$.node.description') AS model_description,
                json_extract(s.snapshot, '$.node.kind') AS model_kind,
                json_extract(s.snapshot, '$.node.grains') AS grain,
                json_extract(s.snapshot, '$.node.columns') AS columns,
                json_extract(s.snapshot, '$.node.column_descriptions') AS column_descriptions,
                json_extract(s.snapshot, '$.node.physical_properties') AS physical_properties
            FROM 
                sqlmesh._snapshots s
            INNER JOIN (
                SELECT name, MAX(updated_ts) AS max_updated_ts
                FROM sqlmesh._snapshots
                GROUP BY name
            ) latest ON s.name = latest.name AND s.updated_ts = latest.max_updated_ts
            ;
        """

        t = con.sql(query)
    except Exception:
        t = ibis.table(schema=COLUMN_SCHEMA)

    return ibis.to_sql(t)
