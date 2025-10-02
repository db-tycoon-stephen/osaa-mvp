"""Schema Registry for managing schema versions and evolution.

This module provides a comprehensive schema versioning system with:
- Schema version tracking and management
- Compatibility validation (backward, forward, full)
- Migration path calculation
- DuckDB-based persistence
- Schema change detection and analysis

Example usage:
    registry = SchemaRegistry()

    # Register a new schema version
    registry.register_schema(
        schema_name="sdg.indicators",
        version=1,
        schema_definition=SDG_SCHEMA_V1,
        compatibility_strategy=CompatibilityStrategy.BACKWARD
    )

    # Get latest schema
    latest = registry.get_latest_schema("sdg.indicators")

    # Validate compatibility
    is_compatible = registry.validate_compatibility(
        schema_name="sdg.indicators",
        new_schema=SDG_SCHEMA_V2,
        from_version=1
    )
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb


class CompatibilityStrategy(Enum):
    """Schema compatibility strategies for evolution."""

    BACKWARD = "backward"  # New schema can read old data (most common)
    FORWARD = "forward"    # Old schema can read new data
    FULL = "full"          # Both backward and forward compatible
    NONE = "none"          # Breaking changes allowed (major version bump)


class SchemaChangeType(Enum):
    """Types of schema changes that can occur."""

    ADD_COLUMN = "add_column"
    REMOVE_COLUMN = "remove_column"
    CHANGE_TYPE = "change_type"
    RENAME_COLUMN = "rename_column"
    CHANGE_NULLABLE = "change_nullable"
    CHANGE_DEFAULT = "change_default"


@dataclass
class SchemaChange:
    """Represents a single schema change between versions."""

    change_type: SchemaChangeType
    column_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    is_breaking: bool = False
    description: str = ""

    def __post_init__(self):
        if not self.description:
            self.description = self._generate_description()

    def _generate_description(self) -> str:
        """Generate human-readable description of the change."""
        if self.change_type == SchemaChangeType.ADD_COLUMN:
            return f"Add column '{self.column_name}' with type {self.new_value.get('type')}"
        elif self.change_type == SchemaChangeType.REMOVE_COLUMN:
            return f"Remove column '{self.column_name}'"
        elif self.change_type == SchemaChangeType.CHANGE_TYPE:
            return f"Change column '{self.column_name}' type from {self.old_value} to {self.new_value}"
        elif self.change_type == SchemaChangeType.RENAME_COLUMN:
            return f"Rename column '{self.column_name}' to '{self.new_value}'"
        elif self.change_type == SchemaChangeType.CHANGE_NULLABLE:
            return f"Change column '{self.column_name}' nullable from {self.old_value} to {self.new_value}"
        elif self.change_type == SchemaChangeType.CHANGE_DEFAULT:
            return f"Change column '{self.column_name}' default from {self.old_value} to {self.new_value}"
        return f"Change to column '{self.column_name}'"


@dataclass
class SchemaVersion:
    """Represents a versioned schema definition."""

    schema_name: str
    version: int
    schema_definition: Dict[str, Dict[str, Any]]
    compatibility_strategy: CompatibilityStrategy
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = "system"
    description: str = ""
    changes_from_previous: List[SchemaChange] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return {
            "schema_name": self.schema_name,
            "version": self.version,
            "schema_definition": json.dumps(self.schema_definition),
            "compatibility_strategy": self.compatibility_strategy.value,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "description": self.description,
            "changes_from_previous": json.dumps([
                {
                    "change_type": c.change_type.value,
                    "column_name": c.column_name,
                    "old_value": c.old_value,
                    "new_value": c.new_value,
                    "is_breaking": c.is_breaking,
                    "description": c.description
                }
                for c in self.changes_from_previous
            ]),
            "metadata": json.dumps(self.metadata)
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaVersion":
        """Create from dictionary."""
        changes = []
        if data.get("changes_from_previous"):
            changes_data = json.loads(data["changes_from_previous"]) if isinstance(
                data["changes_from_previous"], str
            ) else data["changes_from_previous"]

            for c in changes_data:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType(c["change_type"]),
                    column_name=c["column_name"],
                    old_value=c.get("old_value"),
                    new_value=c.get("new_value"),
                    is_breaking=c.get("is_breaking", False),
                    description=c.get("description", "")
                ))

        return cls(
            schema_name=data["schema_name"],
            version=data["version"],
            schema_definition=json.loads(data["schema_definition"]) if isinstance(
                data["schema_definition"], str
            ) else data["schema_definition"],
            compatibility_strategy=CompatibilityStrategy(data["compatibility_strategy"]),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(
                data["created_at"], str
            ) else data["created_at"],
            created_by=data.get("created_by", "system"),
            description=data.get("description", ""),
            changes_from_previous=changes,
            metadata=json.loads(data["metadata"]) if isinstance(
                data.get("metadata", "{}"), str
            ) else data.get("metadata", {})
        )


class SchemaRegistry:
    """Registry for managing schema versions with DuckDB persistence."""

    def __init__(self, registry_path: Optional[str] = None):
        """Initialize schema registry.

        Args:
            registry_path: Path to registry database. If None, uses default path.
        """
        if registry_path is None:
            registry_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                ".schemas",
                "registry.duckdb"
            )

        self.registry_path = Path(registry_path)
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_database()

    def _init_database(self):
        """Initialize the registry database schema."""
        with duckdb.connect(str(self.registry_path)) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS schema_versions (
                    id INTEGER PRIMARY KEY,
                    schema_name VARCHAR NOT NULL,
                    version INTEGER NOT NULL,
                    schema_definition VARCHAR NOT NULL,
                    compatibility_strategy VARCHAR NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    created_by VARCHAR NOT NULL,
                    description VARCHAR,
                    changes_from_previous VARCHAR,
                    metadata VARCHAR,
                    UNIQUE(schema_name, version)
                )
            """)

            con.execute("""
                CREATE SEQUENCE IF NOT EXISTS schema_version_id_seq START 1
            """)

    def register_schema(
        self,
        schema_name: str,
        version: int,
        schema_definition: Dict[str, Dict[str, Any]],
        compatibility_strategy: CompatibilityStrategy = CompatibilityStrategy.BACKWARD,
        description: str = "",
        created_by: str = "system",
        metadata: Optional[Dict[str, Any]] = None
    ) -> SchemaVersion:
        """Register a new schema version.

        Args:
            schema_name: Name of the schema (e.g., 'sdg.indicators')
            version: Version number (must be unique for schema_name)
            schema_definition: Schema definition dictionary
            compatibility_strategy: Compatibility strategy for this version
            description: Human-readable description of changes
            created_by: User or system that created this version
            metadata: Additional metadata

        Returns:
            SchemaVersion object

        Raises:
            ValueError: If version already exists or is invalid
        """
        # Validate version doesn't exist
        existing = self.get_schema(schema_name, version)
        if existing:
            raise ValueError(
                f"Schema {schema_name} version {version} already exists"
            )

        # Calculate changes from previous version
        changes = []
        previous_version = self.get_latest_schema(schema_name)
        if previous_version:
            changes = self._calculate_changes(
                previous_version.schema_definition,
                schema_definition
            )

            # Validate compatibility if required
            if previous_version.compatibility_strategy != CompatibilityStrategy.NONE:
                is_compatible, breaking_changes = self._check_compatibility(
                    changes,
                    previous_version.compatibility_strategy
                )
                if not is_compatible:
                    raise ValueError(
                        f"Schema changes are not compatible with {previous_version.compatibility_strategy.value} strategy. "
                        f"Breaking changes: {[c.description for c in breaking_changes]}"
                    )

        # Create schema version
        schema_version = SchemaVersion(
            schema_name=schema_name,
            version=version,
            schema_definition=schema_definition,
            compatibility_strategy=compatibility_strategy,
            created_by=created_by,
            description=description,
            changes_from_previous=changes,
            metadata=metadata or {}
        )

        # Persist to database
        with duckdb.connect(str(self.registry_path)) as con:
            data = schema_version.to_dict()
            con.execute("""
                INSERT INTO schema_versions (
                    id, schema_name, version, schema_definition,
                    compatibility_strategy, created_at, created_by,
                    description, changes_from_previous, metadata
                )
                VALUES (
                    nextval('schema_version_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, [
                data["schema_name"],
                data["version"],
                data["schema_definition"],
                data["compatibility_strategy"],
                data["created_at"],
                data["created_by"],
                data["description"],
                data["changes_from_previous"],
                data["metadata"]
            ])

        return schema_version

    def get_schema(
        self,
        schema_name: str,
        version: Optional[int] = None
    ) -> Optional[SchemaVersion]:
        """Get a specific schema version.

        Args:
            schema_name: Name of the schema
            version: Version number. If None, returns latest version.

        Returns:
            SchemaVersion or None if not found
        """
        if version is None:
            return self.get_latest_schema(schema_name)

        with duckdb.connect(str(self.registry_path)) as con:
            result = con.execute("""
                SELECT * FROM schema_versions
                WHERE schema_name = ? AND version = ?
            """, [schema_name, version]).fetchone()

            if result:
                columns = [desc[0] for desc in con.description]
                data = dict(zip(columns, result))
                return SchemaVersion.from_dict(data)

        return None

    def get_latest_schema(self, schema_name: str) -> Optional[SchemaVersion]:
        """Get the latest version of a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            Latest SchemaVersion or None if schema not found
        """
        with duckdb.connect(str(self.registry_path)) as con:
            result = con.execute("""
                SELECT * FROM schema_versions
                WHERE schema_name = ?
                ORDER BY version DESC
                LIMIT 1
            """, [schema_name]).fetchone()

            if result:
                columns = [desc[0] for desc in con.description]
                data = dict(zip(columns, result))
                return SchemaVersion.from_dict(data)

        return None

    def list_schemas(self) -> List[str]:
        """List all schema names in the registry.

        Returns:
            List of schema names
        """
        with duckdb.connect(str(self.registry_path)) as con:
            results = con.execute("""
                SELECT DISTINCT schema_name FROM schema_versions
                ORDER BY schema_name
            """).fetchall()

            return [r[0] for r in results]

    def list_versions(self, schema_name: str) -> List[int]:
        """List all versions for a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            List of version numbers
        """
        with duckdb.connect(str(self.registry_path)) as con:
            results = con.execute("""
                SELECT version FROM schema_versions
                WHERE schema_name = ?
                ORDER BY version
            """, [schema_name]).fetchall()

            return [r[0] for r in results]

    def validate_compatibility(
        self,
        schema_name: str,
        new_schema: Dict[str, Dict[str, Any]],
        from_version: Optional[int] = None,
        strategy: Optional[CompatibilityStrategy] = None
    ) -> Tuple[bool, List[SchemaChange]]:
        """Validate if new schema is compatible with existing version.

        Args:
            schema_name: Name of the schema
            new_schema: New schema definition to validate
            from_version: Version to compare against. If None, uses latest.
            strategy: Compatibility strategy to use. If None, uses existing strategy.

        Returns:
            Tuple of (is_compatible, breaking_changes)
        """
        old_version = self.get_schema(schema_name, from_version)
        if not old_version:
            # No existing version, always compatible
            return True, []

        changes = self._calculate_changes(
            old_version.schema_definition,
            new_schema
        )

        compat_strategy = strategy or old_version.compatibility_strategy
        return self._check_compatibility(changes, compat_strategy)

    def get_migration_path(
        self,
        schema_name: str,
        from_version: int,
        to_version: int
    ) -> List[SchemaVersion]:
        """Get the migration path between two versions.

        Args:
            schema_name: Name of the schema
            from_version: Starting version
            to_version: Target version

        Returns:
            List of SchemaVersion objects representing the migration path

        Raises:
            ValueError: If versions don't exist or path is invalid
        """
        if from_version == to_version:
            return []

        with duckdb.connect(str(self.registry_path)) as con:
            if from_version < to_version:
                # Forward migration
                results = con.execute("""
                    SELECT * FROM schema_versions
                    WHERE schema_name = ?
                    AND version > ? AND version <= ?
                    ORDER BY version ASC
                """, [schema_name, from_version, to_version]).fetchall()
            else:
                # Backward migration (rollback)
                results = con.execute("""
                    SELECT * FROM schema_versions
                    WHERE schema_name = ?
                    AND version >= ? AND version < ?
                    ORDER BY version DESC
                """, [schema_name, to_version, from_version]).fetchall()

            if not results:
                raise ValueError(
                    f"No migration path found from version {from_version} to {to_version}"
                )

            columns = [desc[0] for desc in con.description]
            return [
                SchemaVersion.from_dict(dict(zip(columns, row)))
                for row in results
            ]

    def _calculate_changes(
        self,
        old_schema: Dict[str, Dict[str, Any]],
        new_schema: Dict[str, Dict[str, Any]]
    ) -> List[SchemaChange]:
        """Calculate changes between two schema versions.

        Args:
            old_schema: Previous schema definition
            new_schema: New schema definition

        Returns:
            List of SchemaChange objects
        """
        changes = []

        # Check for removed columns
        for col_name in old_schema:
            if col_name not in new_schema:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.REMOVE_COLUMN,
                    column_name=col_name,
                    old_value=old_schema[col_name],
                    is_breaking=True
                ))

        # Check for added and modified columns
        for col_name, new_def in new_schema.items():
            if col_name not in old_schema:
                # New column
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.ADD_COLUMN,
                    column_name=col_name,
                    new_value=new_def,
                    is_breaking=new_def.get("nullable", True) is False
                ))
            else:
                # Check for modifications
                old_def = old_schema[col_name]

                # Type change
                if old_def.get("type") != new_def.get("type"):
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.CHANGE_TYPE,
                        column_name=col_name,
                        old_value=old_def.get("type"),
                        new_value=new_def.get("type"),
                        is_breaking=True
                    ))

                # Nullable change
                if old_def.get("nullable") != new_def.get("nullable"):
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.CHANGE_NULLABLE,
                        column_name=col_name,
                        old_value=old_def.get("nullable"),
                        new_value=new_def.get("nullable"),
                        is_breaking=new_def.get("nullable") is False
                    ))

                # Default change
                if old_def.get("default") != new_def.get("default"):
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.CHANGE_DEFAULT,
                        column_name=col_name,
                        old_value=old_def.get("default"),
                        new_value=new_def.get("default"),
                        is_breaking=False
                    ))

        return changes

    def _check_compatibility(
        self,
        changes: List[SchemaChange],
        strategy: CompatibilityStrategy
    ) -> Tuple[bool, List[SchemaChange]]:
        """Check if changes are compatible with strategy.

        Args:
            changes: List of schema changes
            strategy: Compatibility strategy to validate against

        Returns:
            Tuple of (is_compatible, breaking_changes)
        """
        if strategy == CompatibilityStrategy.NONE:
            return True, []

        breaking_changes = []

        for change in changes:
            if strategy == CompatibilityStrategy.BACKWARD:
                # Backward: new schema can read old data
                if change.change_type == SchemaChangeType.REMOVE_COLUMN:
                    breaking_changes.append(change)
                elif change.change_type == SchemaChangeType.ADD_COLUMN:
                    if change.new_value.get("nullable") is False and "default" not in change.new_value:
                        breaking_changes.append(change)
                elif change.change_type == SchemaChangeType.CHANGE_TYPE:
                    breaking_changes.append(change)
                elif change.change_type == SchemaChangeType.CHANGE_NULLABLE:
                    if change.new_value is False:
                        breaking_changes.append(change)

            elif strategy == CompatibilityStrategy.FORWARD:
                # Forward: old schema can read new data
                if change.change_type == SchemaChangeType.ADD_COLUMN:
                    breaking_changes.append(change)
                elif change.change_type == SchemaChangeType.CHANGE_TYPE:
                    breaking_changes.append(change)

            elif strategy == CompatibilityStrategy.FULL:
                # Full: both backward and forward
                if change.change_type == SchemaChangeType.REMOVE_COLUMN:
                    breaking_changes.append(change)
                elif change.change_type == SchemaChangeType.ADD_COLUMN:
                    if change.new_value.get("nullable") is False:
                        breaking_changes.append(change)
                elif change.change_type == SchemaChangeType.CHANGE_TYPE:
                    breaking_changes.append(change)
                elif change.change_type == SchemaChangeType.CHANGE_NULLABLE:
                    breaking_changes.append(change)

        return len(breaking_changes) == 0, breaking_changes
