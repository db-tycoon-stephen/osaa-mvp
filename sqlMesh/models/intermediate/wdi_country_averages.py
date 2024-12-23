from sqlmesh.core.macros import MacroEvaluator
from sqlmesh import model
import ibis
import os
from macros.ibis_expressions import generate_ibis_table
from macros.s3_paths import s3_transformed_path
from models.intermediate.wdi import column_schema as wdi_column_schema

column_schema = {
    "country_id": "String",
    "indicator_id": "String",
    "year": "Int",
    "value": "String",
    "indicator_label": "String",
    "avg_value_by_country": "Float",
}

# For post statement
schema_to_copy_from = (
    "" if os.getenv("TARGET") == "prod" else f"__{os.getenv('TARGET')}"
)


@model(
    "intermediate.wdi_country_averages_test",
    is_sql=True,
    kind="FULL",
    columns=column_schema,
    post_statements=[
        f"""
        @IF(
            @runtime_stage = 'testing',
                COPY (SELECT * FROM intermediate{schema_to_copy_from}.wdi_country_averages)
                TO {s3_transformed_path(MacroEvaluator, 'osaa_mvp.intermediate.wdi_country_averages')}
                (FORMAT PARQUET)
        );
    """
    ],
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    int_wdi = generate_ibis_table(
        evaluator,
        table_name="wdi",
        schema_name="intermediate",
        column_schema=wdi_column_schema,
    )

    country_averages = (
        int_wdi.filter(
            (int_wdi.value.notnull())
            & (int_wdi.value != "")
            & (int_wdi.value.cast("float").notnull())
        )
        .group_by("country_id")
        .aggregate(avg_value_by_country=int_wdi.value.cast("float").mean())
    )

    final = int_wdi.left_join(country_averages, "country_id")

    return ibis.to_sql(final)
