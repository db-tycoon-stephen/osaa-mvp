import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema
from sqlglot import exp


COLUMN_SCHEMA = {
    "indicator_id": "String",
    "country_id": "String",
    "year": "Int",
    "value": "Decimal",
    "magnitude": "String",
    "qualifier": "String",
    "indicator_description": "String",
    "loaded_at": "Timestamp",
    "file_modified_at": "Timestamp",
}


@model(
    "sources.opri",
    is_sql=True,
    kind="INCREMENTAL_BY_TIME_RANGE",
    time_column="loaded_at",
    columns=COLUMN_SCHEMA,
    grain=("indicator_id", "country_id", "year")
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    source_folder_path = "opri"

    opri_data_national = generate_ibis_table(
        evaluator,
        table_name="data_national",
        column_schema=get_sql_model_schema(evaluator, "data_national", source_folder_path),
        schema_name="opri",
    )

    opri_label = generate_ibis_table(
        evaluator,
        table_name="label",
        column_schema=get_sql_model_schema(evaluator, "label", source_folder_path),
        schema_name="opri",
    )

    opri_table = (
        opri_data_national.left_join(opri_label, "indicator_id")
        .select(
            "indicator_id",
            "country_id",
            "year",
            "value",
            "magnitude",
            "qualifier",
            "indicator_label_en",
            "loaded_at",
            "file_modified_at",
        )
        .rename(indicator_description="indicator_label_en")
    )

    return ibis.to_sql(opri_table)
