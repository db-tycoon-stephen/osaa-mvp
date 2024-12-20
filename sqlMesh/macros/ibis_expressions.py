from sqlmesh import macro
from ibis.expr.operations import Namespace, UnboundTable


@macro()
def generate_ibis_table(
    evaluator,
    table_name: str,
    column_schema: dict,
    schema_name: str,
    database_name: str = "osaa_mvp",
):
    """
    This macro returns an ibis table
    Note that the definition of catalog and database of Namespace class may be different than those of your execution engine
    """
    table = UnboundTable(
        name=table_name,
        schema=column_schema,
        namespace=Namespace(catalog=database_name, database=schema_name),
    ).to_expr()

    return table
