"""Schema Migration Engine for executing schema changes.

This module provides migration operations and execution capabilities:
- Migration operations (AddColumn, RemoveColumn, ChangeType, RenameColumn)
- Migration plan orchestration
- Dry-run support for testing
- Rollback capabilities
- Progress tracking and logging

Example usage:
    # Create migration plan
    migrator = SchemaMigration(
        table_name="sdg.indicators",
        connection_string="duckdb:///data.db"
    )

    # Add migration operations
    plan = MigrationPlan(schema_name="sdg.indicators", from_version=1, to_version=2)
    plan.add_operation(AddColumn("data_source", "String", default="UN"))
    plan.add_operation(AddColumn("confidence_level", "Decimal", nullable=True))

    # Execute with dry-run first
    plan.execute(migrator, dry_run=True)

    # Then execute for real
    plan.execute(migrator, dry_run=False)
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import duckdb

from .schema_registry import SchemaRegistry, SchemaVersion

logger = logging.getLogger(__name__)


class MigrationStatus(Enum):
    """Status of a migration execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class MigrationOperation(ABC):
    """Base class for migration operations."""

    @abstractmethod
    def to_sql(self, table_name: str) -> str:
        """Generate SQL for this operation.

        Args:
            table_name: Full table name (schema.table)

        Returns:
            SQL statement string
        """
        pass

    @abstractmethod
    def get_rollback_operation(self) -> Optional["MigrationOperation"]:
        """Get the operation to rollback this change.

        Returns:
            MigrationOperation that reverses this operation, or None if not reversible
        """
        pass

    @abstractmethod
    def describe(self) -> str:
        """Human-readable description of the operation."""
        pass


class AddColumn(MigrationOperation):
    """Operation to add a new column."""

    def __init__(
        self,
        column_name: str,
        column_type: str,
        nullable: bool = True,
        default: Optional[Any] = None,
        comment: str = ""
    ):
        self.column_name = column_name
        self.column_type = column_type
        self.nullable = nullable
        self.default = default
        self.comment = comment

    def to_sql(self, table_name: str) -> str:
        """Generate ALTER TABLE ADD COLUMN SQL."""
        sql_parts = [f'ALTER TABLE {table_name} ADD COLUMN "{self.column_name}" {self.column_type}']

        if not self.nullable:
            sql_parts.append("NOT NULL")

        if self.default is not None:
            if isinstance(self.default, str):
                sql_parts.append(f"DEFAULT '{self.default}'")
            else:
                sql_parts.append(f"DEFAULT {self.default}")

        return " ".join(sql_parts)

    def get_rollback_operation(self) -> Optional[MigrationOperation]:
        """Return RemoveColumn operation to rollback."""
        return RemoveColumn(self.column_name, backup_column=True)

    def describe(self) -> str:
        """Describe the operation."""
        nullable_str = "nullable" if self.nullable else "not null"
        default_str = f" with default {self.default}" if self.default is not None else ""
        return f"Add column '{self.column_name}' ({self.column_type}, {nullable_str}{default_str})"


class RemoveColumn(MigrationOperation):
    """Operation to remove a column."""

    def __init__(self, column_name: str, backup_column: bool = True):
        self.column_name = column_name
        self.backup_column = backup_column
        self._backup_name = f"{column_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def to_sql(self, table_name: str) -> str:
        """Generate ALTER TABLE DROP COLUMN SQL."""
        if self.backup_column:
            # First rename to backup, then can be dropped later
            return f'ALTER TABLE {table_name} RENAME COLUMN "{self.column_name}" TO "{self._backup_name}"'
        else:
            return f'ALTER TABLE {table_name} DROP COLUMN "{self.column_name}"'

    def get_rollback_operation(self) -> Optional[MigrationOperation]:
        """Cannot automatically rollback column removal without schema info."""
        if self.backup_column:
            return RenameColumn(self._backup_name, self.column_name)
        return None

    def describe(self) -> str:
        """Describe the operation."""
        if self.backup_column:
            return f"Rename column '{self.column_name}' to backup ('{self._backup_name}')"
        return f"Remove column '{self.column_name}'"


class ChangeType(MigrationOperation):
    """Operation to change column type."""

    def __init__(
        self,
        column_name: str,
        old_type: str,
        new_type: str,
        using_expression: Optional[str] = None
    ):
        self.column_name = column_name
        self.old_type = old_type
        self.new_type = new_type
        self.using_expression = using_expression

    def to_sql(self, table_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN TYPE SQL."""
        sql = f'ALTER TABLE {table_name} ALTER COLUMN "{self.column_name}" TYPE {self.new_type}'

        if self.using_expression:
            sql += f" USING {self.using_expression}"

        return sql

    def get_rollback_operation(self) -> Optional[MigrationOperation]:
        """Return ChangeType operation to revert type change."""
        return ChangeType(
            self.column_name,
            old_type=self.new_type,
            new_type=self.old_type
        )

    def describe(self) -> str:
        """Describe the operation."""
        expr_str = f" using {self.using_expression}" if self.using_expression else ""
        return f"Change column '{self.column_name}' type from {self.old_type} to {self.new_type}{expr_str}"


class RenameColumn(MigrationOperation):
    """Operation to rename a column."""

    def __init__(self, old_name: str, new_name: str):
        self.old_name = old_name
        self.new_name = new_name

    def to_sql(self, table_name: str) -> str:
        """Generate ALTER TABLE RENAME COLUMN SQL."""
        return f'ALTER TABLE {table_name} RENAME COLUMN "{self.old_name}" TO "{self.new_name}"'

    def get_rollback_operation(self) -> Optional[MigrationOperation]:
        """Return RenameColumn operation to revert rename."""
        return RenameColumn(self.new_name, self.old_name)

    def describe(self) -> str:
        """Describe the operation."""
        return f"Rename column '{self.old_name}' to '{self.new_name}'"


class ChangeNullable(MigrationOperation):
    """Operation to change column nullable constraint."""

    def __init__(self, column_name: str, nullable: bool, default: Optional[Any] = None):
        self.column_name = column_name
        self.nullable = nullable
        self.default = default

    def to_sql(self, table_name: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN SQL for nullable constraint."""
        if self.nullable:
            return f'ALTER TABLE {table_name} ALTER COLUMN "{self.column_name}" DROP NOT NULL'
        else:
            # When making column NOT NULL, set default first if provided
            sql_statements = []
            if self.default is not None:
                if isinstance(self.default, str):
                    sql_statements.append(
                        f'UPDATE {table_name} SET "{self.column_name}" = \'{self.default}\' '
                        f'WHERE "{self.column_name}" IS NULL'
                    )
                else:
                    sql_statements.append(
                        f'UPDATE {table_name} SET "{self.column_name}" = {self.default} '
                        f'WHERE "{self.column_name}" IS NULL'
                    )

            sql_statements.append(
                f'ALTER TABLE {table_name} ALTER COLUMN "{self.column_name}" SET NOT NULL'
            )
            return "; ".join(sql_statements)

    def get_rollback_operation(self) -> Optional[MigrationOperation]:
        """Return ChangeNullable operation to revert nullable change."""
        return ChangeNullable(self.column_name, not self.nullable)

    def describe(self) -> str:
        """Describe the operation."""
        nullable_str = "nullable" if self.nullable else "not nullable"
        default_str = f" (setting default to {self.default} for nulls)" if self.default and not self.nullable else ""
        return f"Change column '{self.column_name}' to {nullable_str}{default_str}"


@dataclass
class MigrationPlan:
    """Plan for executing a series of migration operations."""

    schema_name: str
    from_version: int
    to_version: int
    operations: List[MigrationOperation] = field(default_factory=list)
    description: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    executed_at: Optional[datetime] = None
    status: MigrationStatus = MigrationStatus.PENDING
    error_message: Optional[str] = None

    def add_operation(self, operation: MigrationOperation):
        """Add an operation to the plan."""
        self.operations.append(operation)

    def get_rollback_plan(self) -> "MigrationPlan":
        """Create a rollback plan to reverse this migration.

        Returns:
            MigrationPlan with rollback operations in reverse order
        """
        rollback_plan = MigrationPlan(
            schema_name=self.schema_name,
            from_version=self.to_version,
            to_version=self.from_version,
            description=f"Rollback: {self.description}"
        )

        # Add rollback operations in reverse order
        for op in reversed(self.operations):
            rollback_op = op.get_rollback_operation()
            if rollback_op:
                rollback_plan.add_operation(rollback_op)
            else:
                logger.warning(f"No rollback available for operation: {op.describe()}")

        return rollback_plan

    def describe(self) -> str:
        """Generate human-readable description of the plan."""
        lines = [
            f"Migration Plan: {self.schema_name}",
            f"From version {self.from_version} to {self.to_version}",
            f"Operations ({len(self.operations)}):"
        ]
        for i, op in enumerate(self.operations, 1):
            lines.append(f"  {i}. {op.describe()}")

        return "\n".join(lines)


class SchemaMigration:
    """Execute schema migrations on a database."""

    def __init__(
        self,
        table_name: str,
        connection_string: Optional[str] = None,
        registry: Optional[SchemaRegistry] = None
    ):
        """Initialize schema migration engine.

        Args:
            table_name: Full table name (schema.table)
            connection_string: DuckDB connection string. If None, uses in-memory.
            registry: SchemaRegistry instance. If None, creates default.
        """
        self.table_name = table_name
        self.connection_string = connection_string or ":memory:"
        self.registry = registry or SchemaRegistry()

    def create_migration_plan(
        self,
        schema_name: str,
        from_version: int,
        to_version: int
    ) -> MigrationPlan:
        """Create a migration plan between two versions.

        Args:
            schema_name: Name of the schema
            from_version: Starting version
            to_version: Target version

        Returns:
            MigrationPlan with operations

        Raises:
            ValueError: If versions don't exist
        """
        # Get migration path from registry
        migration_path = self.registry.get_migration_path(
            schema_name,
            from_version,
            to_version
        )

        plan = MigrationPlan(
            schema_name=schema_name,
            from_version=from_version,
            to_version=to_version,
            description=f"Migrate from v{from_version} to v{to_version}"
        )

        # Build operations from schema changes
        for schema_version in migration_path:
            for change in schema_version.changes_from_previous:
                op = self._change_to_operation(change, schema_version.schema_definition)
                if op:
                    plan.add_operation(op)

        return plan

    def execute_plan(
        self,
        plan: MigrationPlan,
        dry_run: bool = False
    ) -> bool:
        """Execute a migration plan.

        Args:
            plan: MigrationPlan to execute
            dry_run: If True, only log SQL without executing

        Returns:
            True if successful, False otherwise
        """
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
            logger.info(plan.describe())

        plan.status = MigrationStatus.RUNNING

        try:
            with duckdb.connect(self.connection_string) as con:
                for i, operation in enumerate(plan.operations, 1):
                    sql = operation.to_sql(self.table_name)

                    if dry_run:
                        logger.info(f"Operation {i}/{len(plan.operations)}: {operation.describe()}")
                        logger.info(f"SQL: {sql}")
                    else:
                        logger.info(f"Executing operation {i}/{len(plan.operations)}: {operation.describe()}")
                        try:
                            # Handle multi-statement SQL
                            if ";" in sql:
                                for statement in sql.split(";"):
                                    statement = statement.strip()
                                    if statement:
                                        con.execute(statement)
                            else:
                                con.execute(sql)

                            logger.info(f"✓ Operation {i} completed successfully")
                        except Exception as e:
                            logger.error(f"✗ Operation {i} failed: {str(e)}")
                            raise

            if not dry_run:
                plan.executed_at = datetime.now()
                plan.status = MigrationStatus.COMPLETED
                logger.info(f"Migration plan completed successfully: {plan.schema_name} v{plan.from_version} → v{plan.to_version}")
            else:
                logger.info("DRY RUN completed - no changes were made")

            return True

        except Exception as e:
            plan.status = MigrationStatus.FAILED
            plan.error_message = str(e)
            logger.error(f"Migration failed: {str(e)}")
            return False

    def rollback_plan(
        self,
        plan: MigrationPlan,
        dry_run: bool = False
    ) -> bool:
        """Rollback a migration plan.

        Args:
            plan: MigrationPlan to rollback
            dry_run: If True, only log SQL without executing

        Returns:
            True if successful, False otherwise
        """
        rollback_plan = plan.get_rollback_plan()
        logger.info(f"Rolling back migration: {plan.schema_name} v{plan.to_version} → v{plan.from_version}")

        success = self.execute_plan(rollback_plan, dry_run=dry_run)

        if success and not dry_run:
            plan.status = MigrationStatus.ROLLED_BACK
            logger.info("Rollback completed successfully")

        return success

    def _change_to_operation(
        self,
        change,
        schema_definition: Dict[str, Dict[str, Any]]
    ) -> Optional[MigrationOperation]:
        """Convert a SchemaChange to a MigrationOperation.

        Args:
            change: SchemaChange object
            schema_definition: Current schema definition

        Returns:
            MigrationOperation or None
        """
        from .schema_registry import SchemaChangeType

        if change.change_type == SchemaChangeType.ADD_COLUMN:
            col_def = change.new_value
            return AddColumn(
                column_name=change.column_name,
                column_type=col_def.get("type", "String"),
                nullable=col_def.get("nullable", True),
                default=col_def.get("default")
            )

        elif change.change_type == SchemaChangeType.REMOVE_COLUMN:
            return RemoveColumn(
                column_name=change.column_name,
                backup_column=True
            )

        elif change.change_type == SchemaChangeType.CHANGE_TYPE:
            return ChangeType(
                column_name=change.column_name,
                old_type=change.old_value,
                new_type=change.new_value
            )

        elif change.change_type == SchemaChangeType.RENAME_COLUMN:
            return RenameColumn(
                old_name=change.column_name,
                new_name=change.new_value
            )

        elif change.change_type == SchemaChangeType.CHANGE_NULLABLE:
            col_def = schema_definition.get(change.column_name, {})
            return ChangeNullable(
                column_name=change.column_name,
                nullable=change.new_value,
                default=col_def.get("default")
            )

        return None
