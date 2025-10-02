"""Schema utilities for SQLMesh models.

This module provides utilities for integrating the schema registry
with SQLMesh models, enabling versioned schema management.
"""

from typing import Dict, Optional
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlMesh.schemas.sdg import SDG_SCHEMA_V1
from sqlMesh.schemas.opri import OPRI_SCHEMA_V1
from sqlMesh.schemas.wdi import WDI_SCHEMA_V1


def get_schema_for_model(schema_name: str, version: Optional[int] = None) -> Dict[str, str]:
    """Get schema definition for a model.

    Args:
        schema_name: Name of the schema (e.g., 'sdg', 'opri', 'wdi')
        version: Schema version. If None, uses latest (v1 for now)

    Returns:
        Dictionary mapping column names to types for SQLMesh

    Raises:
        ValueError: If schema not found
    """
    # Map schema names to their definitions
    schema_map = {
        'sdg': SDG_SCHEMA_V1,
        'opri': OPRI_SCHEMA_V1,
        'wdi': WDI_SCHEMA_V1,
    }

    if schema_name.lower() not in schema_map:
        raise ValueError(
            f"Schema '{schema_name}' not found. "
            f"Available schemas: {', '.join(schema_map.keys())}"
        )

    schema_def = schema_map[schema_name.lower()]

    # Convert to SQLMesh format (just type names)
    return {
        col_name: col_def["type"]
        for col_name, col_def in schema_def.items()
    }


def get_schema_descriptions(schema_name: str, version: Optional[int] = None) -> Dict[str, str]:
    """Get column descriptions for a schema.

    Args:
        schema_name: Name of the schema (e.g., 'sdg', 'opri', 'wdi')
        version: Schema version. If None, uses latest (v1 for now)

    Returns:
        Dictionary mapping column names to descriptions

    Raises:
        ValueError: If schema not found
    """
    # Map schema names to their definitions
    schema_map = {
        'sdg': SDG_SCHEMA_V1,
        'opri': OPRI_SCHEMA_V1,
        'wdi': WDI_SCHEMA_V1,
    }

    if schema_name.lower() not in schema_map:
        raise ValueError(
            f"Schema '{schema_name}' not found. "
            f"Available schemas: {', '.join(schema_map.keys())}"
        )

    schema_def = schema_map[schema_name.lower()]

    # Extract descriptions
    return {
        col_name: col_def.get("description", "")
        for col_name, col_def in schema_def.items()
        if col_def.get("description")
    }


def get_schema_grain(schema_name: str) -> tuple:
    """Get the grain (primary key columns) for a schema.

    Args:
        schema_name: Name of the schema (e.g., 'sdg', 'opri', 'wdi')

    Returns:
        Tuple of column names that form the grain
    """
    # Define grain for each schema
    grain_map = {
        'sdg': ("indicator_id", "country_id", "year"),
        'opri': ("indicator_id", "country_id", "year"),
        'wdi': ("country_id", "indicator_id", "year"),
    }

    return grain_map.get(schema_name.lower(), ())
