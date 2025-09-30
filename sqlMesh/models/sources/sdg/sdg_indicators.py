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
    "sources.sdg",
    is_sql=True,
    kind="INCREMENTAL_BY_TIME_RANGE",
    time_column="loaded_at",
    columns=COLUMN_SCHEMA,
    description="""This model contains Sustainable Development Goals (SDG) data for all countries and indicators. Uses incremental processing to only process new/updated data.""",
    column_descriptions={
        "indicator_id": "The unique identifier for the indicator",
        "country_id": "The unique identifier for the country",
        "year": "The year of the data",
        "value": "The value of the indicator for the country and year",
        "magnitude": "The magnitude of the indicator for the country and year",
        "qualifier": "The qualifier of the indicator for the country and year",
        "indicator_description": "The description of the indicator",
        "loaded_at": "Timestamp when data entered the pipeline",
        "file_modified_at": "Source file modification timestamp",
    },
    grain=("indicator_id", "country_id", "year"),
    physical_properties={
        "publishing_org": "UN",
        "link_to_raw_data": "https://unstats.un.org/sdgs/dataportal",
        "dataset_owner": "UN",
        "dataset_owner_contact_info": "https://unstats.un.org/sdgs/contact-us/",
        "funding_source": "UN",
        "maintenance_status": "Actively Maintained",
        "how_data_was_collected": "https://unstats.un.org/sdgs/dataContacts/",
        "update_cadence": "Annually",
        "transformations_of_raw_data": "indicator labels and descriptions are joined together",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    source_folder_path = "sdg"

    sdg_data_national = generate_ibis_table(
        evaluator,
        table_name="data_national",
        column_schema=get_sql_model_schema(evaluator, "data_national", source_folder_path),
        schema_name="sdg",
    )

    sdg_label = generate_ibis_table(
        evaluator,
        table_name="label",
        column_schema=get_sql_model_schema(evaluator, "label", source_folder_path),
        schema_name="sdg",
    )

    sdg_table = (
        sdg_data_national.left_join(sdg_label, "indicator_id")
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

    return ibis.to_sql(sdg_table)
