"""Migration definitions for SDG schema evolution.

This module defines migrations between SDG schema versions,
including forward migrations and rollback strategies.
"""

from src.pipeline.schema_migration import AddColumn, MigrationPlan


def create_v1_to_v2_migration(schema_name: str = "sdg.indicators") -> MigrationPlan:
    """Create migration plan from SDG v1 to v2.

    Adds data quality and lineage tracking fields.

    Args:
        schema_name: Full schema name (default: sdg.indicators)

    Returns:
        MigrationPlan object
    """
    plan = MigrationPlan(
        schema_name=schema_name,
        from_version=1,
        to_version=2,
        description="Add data quality tracking fields (data_source, confidence_level)"
    )

    # Add data_source column
    plan.add_operation(AddColumn(
        column_name="data_source",
        column_type="String",
        nullable=True,
        default="UN",
        comment="Source of the data"
    ))

    # Add confidence_level column
    plan.add_operation(AddColumn(
        column_name="confidence_level",
        column_type="Decimal",
        nullable=True,
        comment="Confidence level of the data point (0-1 scale)"
    ))

    return plan


# Migration registry - maps version transitions to migration functions
MIGRATIONS = {
    (1, 2): create_v1_to_v2_migration,
}


def get_migration(from_version: int, to_version: int, schema_name: str = "sdg.indicators") -> MigrationPlan:
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
