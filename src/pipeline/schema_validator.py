"""Schema Validator for compatibility checking and validation.

This module provides comprehensive schema validation capabilities:
- Compatibility checking (backward, forward, full)
- Schema definition validation
- Breaking change identification
- Migration strategy suggestions
- Schema quality checks

Example usage:
    validator = SchemaValidator()

    # Validate compatibility
    is_compatible, issues = validator.validate_compatibility(
        old_schema=v1_schema,
        new_schema=v2_schema,
        strategy=CompatibilityStrategy.BACKWARD
    )

    # Get breaking changes
    breaking_changes = validator.get_breaking_changes(v1_schema, v2_schema)

    # Suggest migration strategy
    suggestion = validator.suggest_migration_strategy(v1_schema, v2_schema)
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from .schema_registry import CompatibilityStrategy, SchemaChange, SchemaChangeType

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Represents a validation issue found in a schema."""

    severity: ValidationSeverity
    message: str
    field: Optional[str] = None
    suggestion: Optional[str] = None

    def __str__(self) -> str:
        field_str = f" [{self.field}]" if self.field else ""
        suggestion_str = f" Suggestion: {self.suggestion}" if self.suggestion else ""
        return f"{self.severity.value.upper()}{field_str}: {self.message}{suggestion_str}"


class SchemaValidator:
    """Validator for schema definitions and compatibility."""

    # Supported data types
    SUPPORTED_TYPES = {
        "String", "Int", "Decimal", "Float", "Boolean",
        "Date", "Timestamp", "Binary", "JSON"
    }

    # Type compatibility matrix (old_type -> compatible_new_types)
    TYPE_COMPATIBILITY = {
        "Int": {"Int", "Decimal", "Float", "String"},
        "Decimal": {"Decimal", "Float", "String"},
        "Float": {"Float", "String"},
        "String": {"String"},
        "Boolean": {"Boolean", "String"},
        "Date": {"Date", "Timestamp", "String"},
        "Timestamp": {"Timestamp", "String"},
        "Binary": {"Binary", "String"},
        "JSON": {"JSON", "String"}
    }

    def validate_schema(
        self,
        schema_definition: Dict[str, Dict[str, Any]],
        strict: bool = True
    ) -> Tuple[bool, List[ValidationIssue]]:
        """Validate a schema definition.

        Args:
            schema_definition: Schema to validate
            strict: If True, warnings are treated as errors

        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []

        # Check for empty schema
        if not schema_definition:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message="Schema cannot be empty"
            ))
            return False, issues

        # Validate each column
        for col_name, col_def in schema_definition.items():
            # Check column name
            if not col_name:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    message="Column name cannot be empty"
                ))
                continue

            if not col_name.replace("_", "").isalnum():
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    field=col_name,
                    message="Column name contains special characters",
                    suggestion="Use only alphanumeric characters and underscores"
                ))

            if col_name.upper() != col_name and col_name != col_name.lower():
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    field=col_name,
                    message="Mixed case column names may cause issues",
                    suggestion="Use lowercase or uppercase consistently"
                ))

            # Check column definition
            if not isinstance(col_def, dict):
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field=col_name,
                    message="Column definition must be a dictionary"
                ))
                continue

            # Check type
            col_type = col_def.get("type")
            if not col_type:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field=col_name,
                    message="Column type is required"
                ))
            elif col_type not in self.SUPPORTED_TYPES:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field=col_name,
                    message=f"Unsupported type '{col_type}'",
                    suggestion=f"Use one of: {', '.join(self.SUPPORTED_TYPES)}"
                ))

            # Check nullable and default
            nullable = col_def.get("nullable", True)
            default = col_def.get("default")

            if not nullable and default is None:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    field=col_name,
                    message="Non-nullable column without default value",
                    suggestion="Provide a default value or make column nullable"
                ))

            # Check default type compatibility
            if default is not None and col_type:
                if not self._validate_default_value(default, col_type):
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        field=col_name,
                        message=f"Default value type may not match column type {col_type}"
                    ))

        # Check for reserved column names
        reserved_names = {"id", "created_at", "updated_at", "deleted_at"}
        for col_name in schema_definition:
            if col_name.lower() in reserved_names:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    field=col_name,
                    message=f"Column name '{col_name}' is commonly reserved",
                    suggestion="Consider using a different name to avoid conflicts"
                ))

        # Determine if valid
        errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]
        warnings = [i for i in issues if i.severity == ValidationSeverity.WARNING]

        is_valid = len(errors) == 0 and (not strict or len(warnings) == 0)

        return is_valid, issues

    def validate_compatibility(
        self,
        old_schema: Dict[str, Dict[str, Any]],
        new_schema: Dict[str, Dict[str, Any]],
        strategy: CompatibilityStrategy
    ) -> Tuple[bool, List[ValidationIssue]]:
        """Validate if new schema is compatible with old schema.

        Args:
            old_schema: Previous schema definition
            new_schema: New schema definition
            strategy: Compatibility strategy to validate against

        Returns:
            Tuple of (is_compatible, list of issues)
        """
        issues = []

        # First validate both schemas
        old_valid, old_issues = self.validate_schema(old_schema, strict=False)
        new_valid, new_issues = self.validate_schema(new_schema, strict=False)

        if not old_valid:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message="Old schema is invalid",
                suggestion=str([str(i) for i in old_issues])
            ))

        if not new_valid:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message="New schema is invalid",
                suggestion=str([str(i) for i in new_issues])
            ))

        if not old_valid or not new_valid:
            return False, issues

        # Calculate changes
        changes = self._calculate_changes(old_schema, new_schema)

        # Check compatibility based on strategy
        for change in changes:
            compat_issues = self._check_change_compatibility(change, strategy)
            issues.extend(compat_issues)

        # Determine if compatible
        errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]
        is_compatible = len(errors) == 0

        return is_compatible, issues

    def get_breaking_changes(
        self,
        old_schema: Dict[str, Dict[str, Any]],
        new_schema: Dict[str, Dict[str, Any]],
        strategy: CompatibilityStrategy = CompatibilityStrategy.BACKWARD
    ) -> List[SchemaChange]:
        """Identify breaking changes between schemas.

        Args:
            old_schema: Previous schema definition
            new_schema: New schema definition
            strategy: Compatibility strategy to check against

        Returns:
            List of breaking SchemaChange objects
        """
        changes = self._calculate_changes(old_schema, new_schema)
        breaking_changes = []

        for change in changes:
            if self._is_breaking_change(change, strategy):
                change.is_breaking = True
                breaking_changes.append(change)

        return breaking_changes

    def suggest_migration_strategy(
        self,
        old_schema: Dict[str, Dict[str, Any]],
        new_schema: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Suggest migration strategy based on schema changes.

        Args:
            old_schema: Previous schema definition
            new_schema: New schema definition

        Returns:
            Dictionary with migration suggestions
        """
        changes = self._calculate_changes(old_schema, new_schema)

        # Analyze changes for each strategy
        strategies_analysis = {}

        for strategy in CompatibilityStrategy:
            breaking = []
            safe = []

            for change in changes:
                if self._is_breaking_change(change, strategy):
                    breaking.append(change)
                else:
                    safe.append(change)

            strategies_analysis[strategy.value] = {
                "compatible": len(breaking) == 0,
                "breaking_changes": len(breaking),
                "safe_changes": len(safe),
                "changes": [change.description for change in breaking]
            }

        # Determine recommended strategy
        if strategies_analysis[CompatibilityStrategy.FULL.value]["compatible"]:
            recommended = CompatibilityStrategy.FULL
            reason = "All changes are fully compatible (backward and forward)"
        elif strategies_analysis[CompatibilityStrategy.BACKWARD.value]["compatible"]:
            recommended = CompatibilityStrategy.BACKWARD
            reason = "Changes are backward compatible (new schema reads old data)"
        elif strategies_analysis[CompatibilityStrategy.FORWARD.value]["compatible"]:
            recommended = CompatibilityStrategy.FORWARD
            reason = "Changes are forward compatible (old schema reads new data)"
        else:
            recommended = CompatibilityStrategy.NONE
            reason = "Breaking changes detected - requires major version bump"

        return {
            "recommended_strategy": recommended.value,
            "reason": reason,
            "strategies_analysis": strategies_analysis,
            "total_changes": len(changes),
            "change_summary": self._summarize_changes(changes)
        }

    def _calculate_changes(
        self,
        old_schema: Dict[str, Dict[str, Any]],
        new_schema: Dict[str, Dict[str, Any]]
    ) -> List[SchemaChange]:
        """Calculate changes between two schema versions."""
        changes = []

        # Check for removed columns
        for col_name in old_schema:
            if col_name not in new_schema:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.REMOVE_COLUMN,
                    column_name=col_name,
                    old_value=old_schema[col_name]
                ))

        # Check for added and modified columns
        for col_name, new_def in new_schema.items():
            if col_name not in old_schema:
                # New column
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.ADD_COLUMN,
                    column_name=col_name,
                    new_value=new_def
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
                        new_value=new_def.get("type")
                    ))

                # Nullable change
                if old_def.get("nullable", True) != new_def.get("nullable", True):
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.CHANGE_NULLABLE,
                        column_name=col_name,
                        old_value=old_def.get("nullable", True),
                        new_value=new_def.get("nullable", True)
                    ))

                # Default change
                if old_def.get("default") != new_def.get("default"):
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.CHANGE_DEFAULT,
                        column_name=col_name,
                        old_value=old_def.get("default"),
                        new_value=new_def.get("default")
                    ))

        return changes

    def _check_change_compatibility(
        self,
        change: SchemaChange,
        strategy: CompatibilityStrategy
    ) -> List[ValidationIssue]:
        """Check if a change is compatible with strategy."""
        issues = []

        if strategy == CompatibilityStrategy.NONE:
            return issues

        if self._is_breaking_change(change, strategy):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field=change.column_name,
                message=f"Breaking change: {change.description}",
                suggestion=self._get_compatibility_suggestion(change, strategy)
            ))
        else:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.INFO,
                field=change.column_name,
                message=f"Compatible change: {change.description}"
            ))

        return issues

    def _is_breaking_change(
        self,
        change: SchemaChange,
        strategy: CompatibilityStrategy
    ) -> bool:
        """Determine if a change is breaking for the given strategy."""
        if strategy == CompatibilityStrategy.NONE:
            return False

        if strategy == CompatibilityStrategy.BACKWARD:
            # Backward: new schema can read old data
            if change.change_type == SchemaChangeType.REMOVE_COLUMN:
                return True
            elif change.change_type == SchemaChangeType.ADD_COLUMN:
                if change.new_value.get("nullable") is False and "default" not in change.new_value:
                    return True
            elif change.change_type == SchemaChangeType.CHANGE_TYPE:
                # Check if type change is compatible
                old_type = change.old_value
                new_type = change.new_value
                if old_type and new_type:
                    compatible_types = self.TYPE_COMPATIBILITY.get(old_type, set())
                    if new_type not in compatible_types:
                        return True
            elif change.change_type == SchemaChangeType.CHANGE_NULLABLE:
                if change.new_value is False:
                    return True

        elif strategy == CompatibilityStrategy.FORWARD:
            # Forward: old schema can read new data
            if change.change_type == SchemaChangeType.ADD_COLUMN:
                return True
            elif change.change_type == SchemaChangeType.CHANGE_TYPE:
                return True

        elif strategy == CompatibilityStrategy.FULL:
            # Full: both backward and forward
            if change.change_type == SchemaChangeType.REMOVE_COLUMN:
                return True
            elif change.change_type == SchemaChangeType.ADD_COLUMN:
                if change.new_value.get("nullable") is False:
                    return True
            elif change.change_type == SchemaChangeType.CHANGE_TYPE:
                return True
            elif change.change_type == SchemaChangeType.CHANGE_NULLABLE:
                return True

        return False

    def _get_compatibility_suggestion(
        self,
        change: SchemaChange,
        strategy: CompatibilityStrategy
    ) -> str:
        """Get suggestion for making a breaking change compatible."""
        if change.change_type == SchemaChangeType.ADD_COLUMN:
            if change.new_value.get("nullable") is False:
                return "Make column nullable or provide a default value"

        elif change.change_type == SchemaChangeType.REMOVE_COLUMN:
            return "Consider deprecating column first, then remove in next major version"

        elif change.change_type == SchemaChangeType.CHANGE_TYPE:
            return "Consider creating a new column with new type and migrate data"

        elif change.change_type == SchemaChangeType.CHANGE_NULLABLE:
            if change.new_value is False:
                return "Provide a default value for existing null values"

        return "Consider creating a new major version for this breaking change"

    def _validate_default_value(self, default: Any, col_type: str) -> bool:
        """Validate that default value matches column type."""
        if default is None:
            return True

        type_validators = {
            "String": lambda x: isinstance(x, str),
            "Int": lambda x: isinstance(x, int),
            "Decimal": lambda x: isinstance(x, (int, float)),
            "Float": lambda x: isinstance(x, (int, float)),
            "Boolean": lambda x: isinstance(x, bool),
        }

        validator = type_validators.get(col_type)
        if validator:
            return validator(default)

        return True  # Unknown type, assume valid

    def _summarize_changes(self, changes: List[SchemaChange]) -> Dict[str, int]:
        """Summarize changes by type."""
        summary = {}
        for change in changes:
            change_type = change.change_type.value
            summary[change_type] = summary.get(change_type, 0) + 1
        return summary
