"""S3 Path Generation Macros for SQLMesh Data Processing.

This module provides utility macros for generating S3 paths for data landing
and transformed datasets. It supports dynamic path generation based on
environment variables and fully qualified table names.

Key features:
- Generate S3 landing paths for raw data
- Generate S3 transformed paths for processed data
- Support for different environments (prod, dev)
- Flexible path construction with username and target support
"""

import os
import typing as t

from sqlglot import exp
from sqlmesh.core.macros import macro, MacroEvaluator


def parse_fully_qualified_name(
    fqtn: t.Union[str, exp.Expression],
) -> t.Tuple[str, str, str]:
    """Parse a fully qualified table name into its components.

    Args:
        fqtn: Fully qualified table name in the format 'database.schema.table'

    Returns:
        A tuple of (database, schema, table)

    Raises:
        ValueError: If the table name is not in the correct format
    """
    # Convert SQLGlot expression to string and clean quotes
    if isinstance(fqtn, exp.Expression):
        if isinstance(fqtn, exp.Identifier):
            fqtn = fqtn.name
        else:
            fqtn = str(fqtn)
    
    # Clean any quotes from the string
    fqtn = fqtn.strip("'\"")

    # Find the first two dots to split database and schema
    first_dot = fqtn.find('.')
    if first_dot == -1:
        raise ValueError("Fully qualified table name must be in the format 'database.schema.table'")
    
    second_dot = fqtn.find('.', first_dot + 1)
    if second_dot == -1:
        raise ValueError("Fully qualified table name must be in the format 'database.schema.table'")
    
    # Extract parts
    database = fqtn[:first_dot].strip("'\"")
    schema = fqtn[first_dot + 1:second_dot].strip("'\"")
    table = fqtn[second_dot + 1:].strip("'\"")
    
    # Remove hash suffix from table name if present
    if "__" in table:
        table = table.rsplit("__", 1)[0]  # Split from right to handle multiple underscores
    
    return database, schema, table


@macro()
def s3_landing_path(
    evaluator: t.Any, subfolder_filename: t.Union[str, exp.Expression]
) -> exp.Literal:
    """Construct S3 landing path.

    Args:
        evaluator: SQLMesh macro evaluator
        subfolder_filename: Name of the subfolder and filename

    Returns:
        S3 path as a SQLGlot literal expression
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "unosaa-data-pipeline")
    target = os.environ.get("TARGET", "prod").lower()
    username = os.environ.get("USERNAME", "default").lower()

    # Use the same environment path logic for both reading and writing
    if target == "prod":
        env_path = "prod"
    else:
        env_path = f"dev/{target}_{username}"  # e.g. dev/dev_username or dev/qa_username

    if isinstance(subfolder_filename, exp.Expression):
        subfolder_filename = str(subfolder_filename).strip("'")

    path = f"s3://{bucket}/{env_path}/landing/{subfolder_filename}.parquet"
    return exp.Literal.string(path)


@macro()
def s3_transformed_path(
    evaluator: t.Any, fqtn: t.Union[str, exp.Expression]
) -> str:
    """Construct S3 transformed path.

    Args:
        evaluator: SQLMesh macro evaluator
        fqtn: Fully qualified table name

    Returns:
        S3 path as a string
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "unosaa-data-pipeline")
    target = os.environ.get("TARGET", "prod").lower()
    username = os.environ.get("USERNAME", "default").lower()

    # Construct the environment path segment based on actual S3 structure
    if target == "prod":
        env_path = "prod"
    else:
        env_path = f"dev/{target}_{username}"  # e.g. dev/dev_username or dev/qa_username

    _, schema, table = parse_fully_qualified_name(fqtn)
    
    # Map schema to source/master based on actual S3 structure
    if schema == "master":
        schema_path = "master"
    else:
        schema_path = "source"
        
    path = f"s3://{bucket}/{env_path}/staging/{schema_path}/{table}.parquet"
    return path


@macro()
def upload_to_s3(evaluator: MacroEvaluator) -> str:
    """Generate a COPY statement for uploading model data to S3.
    
    Args:
        evaluator: SQLMesh macro evaluator
        
    Returns:
        A SQL statement that includes the COPY command, or empty string if not in creation stage
    """
    # # Only generate COPY statement during creation stage
    # if evaluator.runtime_stage != "creating":
    #     return "SELECT 1 WHERE false"  # No-op SQL statement

    # Get @this_model value and clean it
    this_model = str(evaluator.locals.get('this_model', ''))
    this_model = this_model.strip("'\"")
    
    # Get the S3 path directly as a string, which will use the logical table name
    s3_path = s3_transformed_path(evaluator, this_model)
    
    # Build the SQL statement as a single string, ensuring proper quoting
    # Note: We use the physical table name for the SELECT but logical name for the S3 path
    return f"""COPY (SELECT * FROM {this_model}) TO '{s3_path}' (FORMAT PARQUET)""" 

@macro()
def test_post_statement_macro(evaluator: MacroEvaluator) -> str:
    """Generate a simple SQL statement for testing purposes.
    
    Args:
        evaluator: SQLMesh macro evaluator
        
    Returns:
        A simple SQL statement
    """
    return "SELECT 1 as col"
