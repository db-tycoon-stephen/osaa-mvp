"""Migration definitions for OPRI schema evolution.

This module defines migrations between OPRI schema versions,
including forward migrations and rollback strategies.
"""

from src.pipeline.schema_migration import MigrationPlan


# Migration registry - maps version transitions to migration functions
MIGRATIONS = {}


def get_migration(from_version: int, to_version: int, schema_name: str = "opri.indicators") -> MigrationPlan:
    """Get migration plan for version transition.

    Args:
        from_version: Starting version
        to_version: Target version
        schema_name: Full schema name

    Returns:
        MigrationPlan object

    Raises:
        ValueError: If migration path doesn't exist
    """
    migration_key = (from_version, to_version)

    if migration_key not in MIGRATIONS:
        raise ValueError(
            f"No migration defined from version {from_version} to {to_version}. "
            f"Available migrations: {list(MIGRATIONS.keys())}"
        )

    return MIGRATIONS[migration_key](schema_name)
