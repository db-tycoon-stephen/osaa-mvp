import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema
from macros.schema_utils import get_schema_for_model, get_schema_descriptions, get_schema_grain
from sqlglot import exp


# Use versioned schema from registry (v1)
COLUMN_SCHEMA = get_schema_for_model("opri", version=1)
COLUMN_DESCRIPTIONS = get_schema_descriptions("opri", version=1)
SCHEMA_GRAIN = get_schema_grain("opri")


@model(
    "sources.opri",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="""This model contains OPRI (Open-source Policy Research Institute) indicators.

    Schema Version: 1
    Managed by schema registry for version control and evolution.
    """,
    column_descriptions=COLUMN_DESCRIPTIONS,
    grain=SCHEMA_GRAIN,
    physical_properties={
        "schema_version": 1,  # Track schema version
    },
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
        )
        .rename(indicator_description="indicator_label_en")
    )

    return ibis.to_sql(opri_table)
