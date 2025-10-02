#!/usr/bin/env python3
"""Schema Manager CLI - Manage schema versions and migrations.

This CLI tool provides comprehensive schema management capabilities:
- List all schemas and versions
- Display schema details
- Show differences between versions
- Execute migrations with dry-run support
- Validate schema compatibility
- Register new schema versions
- Rollback to previous versions

Example usage:
    # List all schemas
    python scripts/schema_manager.py list

    # Show specific schema version
    python scripts/schema_manager.py show sdg.indicators --version 1

    # Compare versions
    python scripts/schema_manager.py diff sdg.indicators 1 2

    # Validate new schema
    python scripts/schema_manager.py validate sdg.indicators --file schemas/sdg/v3.py

    # Execute migration
    python scripts/schema_manager.py migrate sdg.indicators --from 1 --to 2 --dry-run
    python scripts/schema_manager.py migrate sdg.indicators --from 1 --to 2

    # Register new schema version
    python scripts/schema_manager.py register sdg.indicators --version 2 --file schemas/sdg/v2.py

    # Rollback to previous version
    python scripts/schema_manager.py rollback sdg.indicators --from 2 --to 1
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline.schema_registry import (
    SchemaRegistry,
    CompatibilityStrategy,
    SchemaChangeType
)
from src.pipeline.schema_migration import SchemaMigration
from src.pipeline.schema_validator import SchemaValidator, ValidationSeverity


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_header(text: str):
    """Print formatted header."""
    print(f"\n{Colors.BOLD}{Colors.HEADER}{text}{Colors.ENDC}")


def print_success(text: str):
    """Print success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")


def print_error(text: str):
    """Print error message."""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}", file=sys.stderr)


def print_warning(text: str):
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.ENDC}")


def print_info(text: str):
    """Print info message."""
    print(f"{Colors.CYAN}ℹ {text}{Colors.ENDC}")


def list_schemas(args):
    """List all schemas in the registry."""
    registry = SchemaRegistry()
    schemas = registry.list_schemas()

    if not schemas:
        print_info("No schemas found in registry")
        return

    print_header("Registered Schemas")

    for schema_name in schemas:
        versions = registry.list_versions(schema_name)
        latest = registry.get_latest_schema(schema_name)

        print(f"\n{Colors.BOLD}{schema_name}{Colors.ENDC}")
        print(f"  Versions: {', '.join(map(str, versions))}")
        if latest:
            print(f"  Latest: v{latest.version} ({latest.compatibility_strategy.value})")
            print(f"  Created: {latest.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            if latest.description:
                print(f"  Description: {latest.description}")


def show_schema(args):
    """Show details of a specific schema version."""
    registry = SchemaRegistry()
    schema = registry.get_schema(args.schema_name, args.version)

    if not schema:
        print_error(f"Schema {args.schema_name} version {args.version or 'latest'} not found")
        sys.exit(1)

    print_header(f"Schema: {schema.schema_name} (v{schema.version})")

    print(f"\n{Colors.BOLD}Metadata:{Colors.ENDC}")
    print(f"  Version: {schema.version}")
    print(f"  Compatibility Strategy: {schema.compatibility_strategy.value}")
    print(f"  Created: {schema.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Created By: {schema.created_by}")
    if schema.description:
        print(f"  Description: {schema.description}")

    print(f"\n{Colors.BOLD}Schema Definition:{Colors.ENDC}")
    for col_name, col_def in schema.schema_definition.items():
        nullable_str = "NULL" if col_def.get("nullable", True) else "NOT NULL"
        default_str = f" DEFAULT {col_def.get('default')}" if col_def.get("default") is not None else ""
        print(f"  {col_name}: {col_def.get('type')} {nullable_str}{default_str}")
        if col_def.get("description"):
            print(f"    → {col_def['description']}")

    if schema.changes_from_previous:
        print(f"\n{Colors.BOLD}Changes from Previous Version:{Colors.ENDC}")
        for change in schema.changes_from_previous:
            icon = "🔴" if change.is_breaking else "🟢"
            print(f"  {icon} {change.description}")

    if args.json:
        print(f"\n{Colors.BOLD}JSON Output:{Colors.ENDC}")
        print(json.dumps(schema.to_dict(), indent=2))


def diff_schemas(args):
    """Show differences between two schema versions."""
    registry = SchemaRegistry()

    old_schema = registry.get_schema(args.schema_name, args.from_version)
    new_schema = registry.get_schema(args.schema_name, args.to_version)

    if not old_schema:
        print_error(f"Schema {args.schema_name} version {args.from_version} not found")
        sys.exit(1)

    if not new_schema:
        print_error(f"Schema {args.schema_name} version {args.to_version} not found")
        sys.exit(1)

    print_header(f"Schema Diff: {args.schema_name} v{args.from_version} → v{args.to_version}")

    # Calculate changes
    validator = SchemaValidator()
    changes = validator._calculate_changes(
        old_schema.schema_definition,
        new_schema.schema_definition
    )

    if not changes:
        print_success("No changes between versions")
        return

    # Group changes by type
    changes_by_type = {}
    for change in changes:
        change_type = change.change_type.value
        if change_type not in changes_by_type:
            changes_by_type[change_type] = []
        changes_by_type[change_type].append(change)

    # Display changes
    for change_type, type_changes in changes_by_type.items():
        print(f"\n{Colors.BOLD}{change_type.upper()} ({len(type_changes)}):{Colors.ENDC}")
        for change in type_changes:
            icon = "🔴" if change.is_breaking else "🟢"
            print(f"  {icon} {change.description}")

    # Compatibility analysis
    print(f"\n{Colors.BOLD}Compatibility Analysis:{Colors.ENDC}")

    for strategy in CompatibilityStrategy:
        is_compatible, issues = validator.validate_compatibility(
            old_schema.schema_definition,
            new_schema.schema_definition,
            strategy
        )

        status_icon = "✓" if is_compatible else "✗"
        status_color = Colors.GREEN if is_compatible else Colors.RED
        print(f"  {status_color}{status_icon} {strategy.value.upper()}: {'Compatible' if is_compatible else 'Incompatible'}{Colors.ENDC}")


def validate_schema(args):
    """Validate a schema definition or compatibility."""
    registry = SchemaRegistry()
    validator = SchemaValidator()

    # Load schema from file
    schema_def = load_schema_from_file(args.file)

    print_header(f"Validating Schema: {args.schema_name}")

    # Validate schema definition
    is_valid, issues = validator.validate_schema(schema_def, strict=args.strict)

    print(f"\n{Colors.BOLD}Schema Definition Validation:{Colors.ENDC}")

    if is_valid:
        print_success("Schema definition is valid")
    else:
        print_error("Schema definition has issues")

    for issue in issues:
        if issue.severity == ValidationSeverity.ERROR:
            print_error(str(issue))
        elif issue.severity == ValidationSeverity.WARNING:
            print_warning(str(issue))
        else:
            print_info(str(issue))

    # Check compatibility if comparing to existing version
    if args.compare_to:
        existing_schema = registry.get_schema(args.schema_name, args.compare_to)
        if not existing_schema:
            print_error(f"Comparison version {args.compare_to} not found")
            sys.exit(1)

        strategy = CompatibilityStrategy(args.strategy)
        is_compatible, compat_issues = validator.validate_compatibility(
            existing_schema.schema_definition,
            schema_def,
            strategy
        )

        print(f"\n{Colors.BOLD}Compatibility Check (v{args.compare_to} → new, {strategy.value}):{Colors.ENDC}")

        if is_compatible:
            print_success(f"Schema is compatible with {strategy.value} strategy")
        else:
            print_error(f"Schema is NOT compatible with {strategy.value} strategy")

        for issue in compat_issues:
            if issue.severity == ValidationSeverity.ERROR:
                print_error(str(issue))
            elif issue.severity == ValidationSeverity.WARNING:
                print_warning(str(issue))
            else:
                print_info(str(issue))

        # Suggest migration strategy
        suggestion = validator.suggest_migration_strategy(
            existing_schema.schema_definition,
            schema_def
        )

        print(f"\n{Colors.BOLD}Migration Strategy Suggestion:{Colors.ENDC}")
        print(f"  Recommended: {suggestion['recommended_strategy']}")
        print(f"  Reason: {suggestion['reason']}")

    sys.exit(0 if is_valid else 1)


def migrate_schema(args):
    """Execute a migration between schema versions."""
    registry = SchemaRegistry()

    # Create migration plan
    migrator = SchemaMigration(
        table_name=args.schema_name.replace(".", "__"),
        connection_string=args.db_connection,
        registry=registry
    )

    try:
        plan = migrator.create_migration_plan(
            args.schema_name,
            args.from_version,
            args.to_version
        )
    except ValueError as e:
        print_error(str(e))
        sys.exit(1)

    print_header(f"Migration Plan: {args.schema_name} v{args.from_version} → v{args.to_version}")

    # Display plan
    print(f"\n{Colors.BOLD}Operations:{Colors.ENDC}")
    for i, op in enumerate(plan.operations, 1):
        print(f"  {i}. {op.describe()}")

    if args.dry_run:
        print_warning("\nDRY RUN MODE - No changes will be made")
    elif not args.yes:
        response = input(f"\n{Colors.YELLOW}Proceed with migration? [y/N]: {Colors.ENDC}")
        if response.lower() != 'y':
            print_info("Migration cancelled")
            sys.exit(0)

    # Execute migration
    print(f"\n{Colors.BOLD}Executing Migration:{Colors.ENDC}")
    success = migrator.execute_plan(plan, dry_run=args.dry_run)

    if success:
        if args.dry_run:
            print_success("Dry run completed successfully")
        else:
            print_success("Migration completed successfully")
        sys.exit(0)
    else:
        print_error("Migration failed")
        sys.exit(1)


def register_schema(args):
    """Register a new schema version."""
    registry = SchemaRegistry()

    # Load schema from file
    schema_def = load_schema_from_file(args.file)

    # Validate schema (non-strict for registration)
    validator = SchemaValidator()
    is_valid, issues = validator.validate_schema(schema_def, strict=False)

    if not is_valid:
        print_error("Schema validation failed")
        for issue in issues:
            print_error(str(issue))
        sys.exit(1)

    # Register schema
    strategy = CompatibilityStrategy(args.strategy)

    try:
        schema_version = registry.register_schema(
            schema_name=args.schema_name,
            version=args.version,
            schema_definition=schema_def,
            compatibility_strategy=strategy,
            description=args.description or "",
            created_by=args.created_by or "cli"
        )

        print_success(f"Schema {args.schema_name} v{args.version} registered successfully")
        print(f"  Compatibility Strategy: {strategy.value}")
        print(f"  Changes from previous: {len(schema_version.changes_from_previous)}")

        if schema_version.changes_from_previous:
            print(f"\n{Colors.BOLD}Changes:{Colors.ENDC}")
            for change in schema_version.changes_from_previous:
                icon = "🔴" if change.is_breaking else "🟢"
                print(f"  {icon} {change.description}")

    except ValueError as e:
        print_error(str(e))
        sys.exit(1)


def rollback_schema(args):
    """Rollback a schema to a previous version."""
    registry = SchemaRegistry()

    # Create migration plan
    migrator = SchemaMigration(
        table_name=args.schema_name.replace(".", "__"),
        connection_string=args.db_connection,
        registry=registry
    )

    try:
        plan = migrator.create_migration_plan(
            args.schema_name,
            args.from_version,
            args.to_version
        )
    except ValueError as e:
        print_error(str(e))
        sys.exit(1)

    print_header(f"Rollback Plan: {args.schema_name} v{args.from_version} → v{args.to_version}")
    print_warning("This will revert schema changes")

    # Display rollback plan
    rollback_plan = plan.get_rollback_plan()

    print(f"\n{Colors.BOLD}Rollback Operations:{Colors.ENDC}")
    for i, op in enumerate(rollback_plan.operations, 1):
        print(f"  {i}. {op.describe()}")

    if args.dry_run:
        print_warning("\nDRY RUN MODE - No changes will be made")
    elif not args.yes:
        response = input(f"\n{Colors.YELLOW}Proceed with rollback? [y/N]: {Colors.ENDC}")
        if response.lower() != 'y':
            print_info("Rollback cancelled")
            sys.exit(0)

    # Execute rollback
    print(f"\n{Colors.BOLD}Executing Rollback:{Colors.ENDC}")
    success = migrator.rollback_plan(plan, dry_run=args.dry_run)

    if success:
        if args.dry_run:
            print_success("Dry run completed successfully")
        else:
            print_success("Rollback completed successfully")
        sys.exit(0)
    else:
        print_error("Rollback failed")
        sys.exit(1)


def load_schema_from_file(file_path: str) -> Dict[str, Any]:
    """Load schema definition from Python file.

    Args:
        file_path: Path to Python file containing schema definition

    Returns:
        Schema definition dictionary

    Raises:
        SystemExit: If file cannot be loaded
    """
    import importlib.util
    import os

    try:
        # Add parent directory to sys.path for relative imports
        parent_dir = os.path.dirname(os.path.abspath(file_path))
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)

        spec = importlib.util.spec_from_file_location("schema_module", file_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules["schema_module"] = module  # Add to sys.modules for imports
            spec.loader.exec_module(module)

            # Look for schema definition - get the highest version number
            schema_candidates = []
            for attr_name in dir(module):
                # Match patterns like SDG_SCHEMA_V1, SCHEMA_V1, etc.
                if ("_SCHEMA_V" in attr_name or attr_name.startswith("SCHEMA_V")) and isinstance(getattr(module, attr_name), dict):
                    schema_candidates.append(attr_name)

            if schema_candidates:
                # Sort by version number (extract number from name)
                import re
                def get_version_num(name):
                    match = re.search(r'V(\d+)', name)
                    return int(match.group(1)) if match else 0

                schema_candidates.sort(key=get_version_num, reverse=True)
                schema_name = schema_candidates[0]
                schema = getattr(module, schema_name)
                # Clean up
                if parent_dir in sys.path:
                    sys.path.remove(parent_dir)
                return schema

            print_error(f"No schema definition found in {file_path}")
            print_error(f"Available attributes: {[a for a in dir(module) if not a.startswith('_')]}")
            sys.exit(1)
        else:
            print_error(f"Cannot load module from {file_path}")
            sys.exit(1)

    except Exception as e:
        print_error(f"Error loading schema from file: {str(e)}")
        # Clean up on error
        if parent_dir in sys.path:
            sys.path.remove(parent_dir)
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Schema Manager CLI - Manage schema versions and migrations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # List command
    list_parser = subparsers.add_parser("list", help="List all schemas and versions")
    list_parser.set_defaults(func=list_schemas)

    # Show command
    show_parser = subparsers.add_parser("show", help="Show schema details")
    show_parser.add_argument("schema_name", help="Schema name (e.g., sdg.indicators)")
    show_parser.add_argument("--version", type=int, help="Schema version (default: latest)")
    show_parser.add_argument("--json", action="store_true", help="Output as JSON")
    show_parser.set_defaults(func=show_schema)

    # Diff command
    diff_parser = subparsers.add_parser("diff", help="Show differences between versions")
    diff_parser.add_argument("schema_name", help="Schema name")
    diff_parser.add_argument("from_version", type=int, help="From version")
    diff_parser.add_argument("to_version", type=int, help="To version")
    diff_parser.set_defaults(func=diff_schemas)

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate schema definition")
    validate_parser.add_argument("schema_name", help="Schema name")
    validate_parser.add_argument("--file", required=True, help="Path to schema definition file")
    validate_parser.add_argument("--strict", action="store_true", help="Strict validation (warnings as errors)")
    validate_parser.add_argument("--compare-to", type=int, help="Compare compatibility to version")
    validate_parser.add_argument("--strategy", default="backward",
                                 choices=[s.value for s in CompatibilityStrategy],
                                 help="Compatibility strategy")
    validate_parser.set_defaults(func=validate_schema)

    # Migrate command
    migrate_parser = subparsers.add_parser("migrate", help="Execute migration")
    migrate_parser.add_argument("schema_name", help="Schema name")
    migrate_parser.add_argument("--from", dest="from_version", type=int, required=True,
                                help="From version")
    migrate_parser.add_argument("--to", dest="to_version", type=int, required=True,
                                help="To version")
    migrate_parser.add_argument("--dry-run", action="store_true", help="Dry run (no changes)")
    migrate_parser.add_argument("--yes", action="store_true", help="Skip confirmation")
    migrate_parser.add_argument("--db-connection", default=":memory:",
                                help="Database connection string")
    migrate_parser.set_defaults(func=migrate_schema)

    # Register command
    register_parser = subparsers.add_parser("register", help="Register new schema version")
    register_parser.add_argument("schema_name", help="Schema name")
    register_parser.add_argument("--version", type=int, required=True, help="Schema version")
    register_parser.add_argument("--file", required=True, help="Path to schema definition file")
    register_parser.add_argument("--strategy", default="backward",
                                 choices=[s.value for s in CompatibilityStrategy],
                                 help="Compatibility strategy")
    register_parser.add_argument("--description", help="Version description")
    register_parser.add_argument("--created-by", help="Creator name")
    register_parser.set_defaults(func=register_schema)

    # Rollback command
    rollback_parser = subparsers.add_parser("rollback", help="Rollback to previous version")
    rollback_parser.add_argument("schema_name", help="Schema name")
    rollback_parser.add_argument("--from", dest="from_version", type=int, required=True,
                                 help="From version")
    rollback_parser.add_argument("--to", dest="to_version", type=int, required=True,
                                 help="To version")
    rollback_parser.add_argument("--dry-run", action="store_true", help="Dry run (no changes)")
    rollback_parser.add_argument("--yes", action="store_true", help="Skip confirmation")
    rollback_parser.add_argument("--db-connection", default=":memory:",
                                 help="Database connection string")
    rollback_parser.set_defaults(func=rollback_schema)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
