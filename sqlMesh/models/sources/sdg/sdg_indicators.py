import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from macros.ibis_expressions import generate_ibis_table
from macros.utils import get_sql_model_schema
from macros.schema_utils import get_schema_for_model, get_schema_descriptions, get_schema_grain
from sqlglot import exp


# Use versioned schema from registry (v1)
COLUMN_SCHEMA = get_schema_for_model("sdg", version=1)
COLUMN_DESCRIPTIONS = get_schema_descriptions("sdg", version=1)
SCHEMA_GRAIN = get_schema_grain("sdg")


@model(
    "sources.sdg",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="""This model contains Sustainable Development Goals (SDG) data for all countries and indicators.

    Schema Version: 1
    Managed by schema registry for version control and evolution.
    """,
    column_descriptions=COLUMN_DESCRIPTIONS,
    grain=SCHEMA_GRAIN,
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
        "schema_version": 1,  # Track schema version
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
        )
        .rename(indicator_description="indicator_label_en")
    )

    return ibis.to_sql(sdg_table)
