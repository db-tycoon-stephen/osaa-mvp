from sqlmesh import macro
from ibis.expr.operations import Namespace, UnboundTable
import ibis.expr.datatypes as dt
# from sqlglot import exp
# import ibis
# import duckdb
# from sqlglot import parse_one, exp, transpile
# from constants import DB_PATH

# def map_to_schema_type(duckdb_type):
#         # Convert to string and uppercase for consistency
#         type_str = str(duckdb_type).upper()
        
#         # Extract base type by removing anything after '(' if it exists
#         # Handling dtype such as DECIMAL(18,3)
#         base_type = type_str.split('(')[0].strip()
        
#         type_mapping = {
#             'TEXT': dt.String,
#             'VARCHAR': dt.String,
#             'CHAR': dt.String,
#             'INT': dt.Int64,
#             'INTEGER': dt.Int64,
#             'BIGINT': dt.Int64,
#             'DECIMAL': dt.Decimal,
#             'NUMERIC': dt.Decimal
#         }
#         return type_mapping.get(base_type, 'String')  # Default to String if unknown

# def get_table_schema(table_expr: str, connection, from_dialect: str = "duckdb", to_dialect: str = "duckdb") -> dict:
#     """
#     Get column names and data types using sqlglot dialect translation
#     Returns dict of {column_name: data_type}
#     """
#     # Create DESCRIBE query and transpile to target dialect
#     describe_query = f"DESCRIBE {table_expr}"
#     translated_query = transpile(describe_query, read=from_dialect, write=to_dialect)[0]
    
#     # For databases that don't support DESCRIBE, use information_schema
#     if to_dialect in ('postgres', 'snowflake', 'bigquery'):
#         table = parse_one(f"SELECT * FROM {table_expr}").find(exp.Table)
#         schema_name = table.catalog or table.db
#         table_name = table.name
        
#         translated_query = transpile(f"""
#             SELECT column_name, data_type 
#             FROM information_schema.columns 
#             WHERE table_name = '{table_name}'
#             {f"AND table_schema = '{schema_name}'" if schema_name else ''}
#         """, read=from_dialect, write=to_dialect)[0]

#     results = connection.execute(translated_query).fetchall()
#     return {row[0]: map_to_schema_type(row[1]) for row in results}
    

# @macro
def get_upstream_columns_and_types(evaluator, model_name: str):
    
    columns_to_types = evaluator.columns_to_types(model_name)
    def map_to_schema_type(duckdb_type):
        # Convert to string and uppercase for consistency
        type_str = str(duckdb_type).upper()
        
        # Extract base type by removing anything after '(' if it exists
        # Handling dtype such as DECIMAL(18,3)
        base_type = type_str.split('(')[0].strip()
        
        type_mapping = {
            'TEXT': dt.String,
            'VARCHAR': dt.String,
            'CHAR': dt.String,
            'INT': dt.Int64,
            'INTEGER': dt.Int64,
            'BIGINT': dt.Int64,
            'DECIMAL': dt.Decimal,
            'NUMERIC': dt.Decimal
        }
        return type_mapping.get(base_type, 'String')  # Default to String if unknown

    return {name: map_to_schema_type(str(type)) for name, type in columns_to_types.items()}

@macro()
def generate_ibis_table(
    evaluator,
    table_name: str,
    schema_name: str,
    column_schema: dict,
    database_name: str = "osaa_mvp",
):
    """
    This macro returns an ibis table
    Note that the definition of catalog and database of Namespace class may be different than those of your execution engine
    """

    model_name = schema_name + "." + table_name

    if evaluator.runtime_stage == "loading":
        print("--- Using the fixed schema")
        table = UnboundTable(
            name=table_name,
            schema=column_schema,
            namespace=Namespace(catalog=database_name, database=schema_name),
        ).to_expr()

        return table

    elif evaluator.runtime_stage in ["evaluating", "creating", "testing"]:
        print("--- Using the hema")
        table = UnboundTable(
            name=table_name,
            schema=column_schema,# get_upstream_columns_and_types(evaluator, model_name),
            namespace=Namespace(catalog=database_name, database=schema_name),
        ).to_expr()

        return table
    else:
        print(f"Skipping table creation - runtime stage is not 'loading' or 'evaluating'")
        return None

