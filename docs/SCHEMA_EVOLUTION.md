# Schema Evolution and Versioning Strategy

## Table of Contents

- [Overview](#overview)
- [Schema Versioning System](#schema-versioning-system)
- [Compatibility Strategies](#compatibility-strategies)
- [Migration Process](#migration-process)
- [Schema Registry](#schema-registry)
- [CLI Tools](#cli-tools)
- [CI/CD Integration](#cicd-integration)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Overview

The OSAA Data Pipeline implements a comprehensive schema evolution and versioning strategy to manage data model changes over time. This system enables:

- **Version Control**: Track all schema changes with full history
- **Compatibility Validation**: Ensure changes don't break existing consumers
- **Automated Migrations**: Execute schema changes safely with rollback support
- **CI/CD Integration**: Validate schema changes in pull requests
- **Documentation**: Maintain clear schema documentation and change history

### Architecture

The schema evolution system consists of four main components:

1. **Schema Registry** (`src/pipeline/schema_registry.py`): Central repository for schema versions
2. **Schema Migration** (`src/pipeline/schema_migration.py`): Migration execution engine
3. **Schema Validator** (`src/pipeline/schema_validator.py`): Compatibility checking and validation
4. **Schema Manager CLI** (`scripts/schema_manager.py`): Command-line interface for management

## Schema Versioning System

### Version Numbering

Schemas use integer versioning (v1, v2, v3, etc.):

- **v1**: Initial schema version
- **v2+**: Incremental changes to the schema

Version numbers are sequential and managed by the schema registry.

### Schema Definition Format

Schemas are defined as Python dictionaries with the following structure:

```python
SCHEMA_V1 = {
    "column_name": {
        "type": "String|Int|Decimal|Float|Boolean|Date|Timestamp|Binary|JSON",
        "nullable": True|False,
        "default": <default_value>,  # Optional
        "description": "Column description"
    },
    # ... more columns
}
```

### Supported Data Types

- **String**: Text data
- **Int**: Integer numbers
- **Decimal**: Decimal numbers (precise)
- **Float**: Floating-point numbers
- **Boolean**: True/False values
- **Date**: Date values
- **Timestamp**: Date and time values
- **Binary**: Binary data
- **JSON**: JSON-encoded data

### Schema Metadata

Each schema version includes metadata:

```python
SCHEMA_V1_METADATA = {
    "version": 1,
    "created_at": "2024-10-01",
    "created_by": "system",
    "description": "Initial schema with core fields",
    "grain": ["primary", "key", "columns"],
    "source": {
        "publishing_org": "Organization",
        "link_to_raw_data": "https://...",
        "update_cadence": "Annually"
    }
}
```

## Compatibility Strategies

The system supports four compatibility strategies:

### 1. BACKWARD (Default)

**New schema can read old data**

- ✅ Safe changes:
  - Add optional columns (nullable or with default)
  - Add enum values
  - Relax constraints (make column nullable)

- ❌ Breaking changes:
  - Remove columns
  - Add required columns without default
  - Change column type incompatibly
  - Make column non-nullable

**Use case**: Most common strategy for evolving schemas

### 2. FORWARD

**Old schema can read new data**

- ✅ Safe changes:
  - Remove columns
  - Narrow types (if compatible)

- ❌ Breaking changes:
  - Add any column
  - Change types

**Use case**: When you need to deploy schema changes before code changes

### 3. FULL

**Both backward and forward compatible**

- ✅ Safe changes:
  - Add optional columns only

- ❌ Breaking changes:
  - Remove columns
  - Add required columns
  - Change types
  - Change nullable constraints

**Use case**: When you need maximum compatibility

### 4. NONE

**Breaking changes allowed**

- ✅ All changes allowed
- Requires major version bump
- May require coordinated deployments

**Use case**: Major refactoring or redesign

## Migration Process

### Migration Operations

The system supports the following migration operations:

#### AddColumn

Add a new column to the schema:

```python
AddColumn(
    column_name="new_field",
    column_type="String",
    nullable=True,
    default="default_value",
    comment="Field description"
)
```

#### RemoveColumn

Remove a column (with optional backup):

```python
RemoveColumn(
    column_name="old_field",
    backup_column=True  # Renames to backup instead of dropping
)
```

#### ChangeType

Change column data type:

```python
ChangeType(
    column_name="field",
    old_type="String",
    new_type="Int",
    using_expression="CAST(field AS INT)"  # Optional
)
```

#### RenameColumn

Rename a column:

```python
RenameColumn(
    old_name="old_field",
    new_name="new_field"
)
```

#### ChangeNullable

Change nullable constraint:

```python
ChangeNullable(
    column_name="field",
    nullable=False,
    default="default_for_nulls"  # Optional
)
```

### Migration Execution

Migrations can be executed with dry-run support:

```bash
# Dry run (no changes)
python scripts/schema_manager.py migrate sdg.indicators --from 1 --to 2 --dry-run

# Execute migration
python scripts/schema_manager.py migrate sdg.indicators --from 1 --to 2

# Auto-confirm (for automation)
python scripts/schema_manager.py migrate sdg.indicators --from 1 --to 2 --yes
```

### Rollback

Migrations can be rolled back to previous versions:

```bash
# Dry run rollback
python scripts/schema_manager.py rollback sdg.indicators --from 2 --to 1 --dry-run

# Execute rollback
python scripts/schema_manager.py rollback sdg.indicators --from 2 --to 1
```

## Schema Registry

### Registry Storage

The schema registry uses DuckDB for persistence:

- **Location**: `.schemas/registry.duckdb`
- **Structure**: Relational tables storing schema versions and metadata
- **Querying**: SQL-based querying for version history

### Registry Operations

#### List Schemas

```bash
python scripts/schema_manager.py list
```

Output:
```
sdg.indicators
  Versions: 1, 2
  Latest: v2 (backward)
  Created: 2024-10-01 10:30:00
```

#### Show Schema Details

```bash
python scripts/schema_manager.py show sdg.indicators --version 1
```

#### Compare Versions

```bash
python scripts/schema_manager.py diff sdg.indicators 1 2
```

#### Register New Version

```bash
python scripts/schema_manager.py register sdg.indicators \
  --version 2 \
  --file sqlMesh/schemas/sdg/v2.py \
  --strategy backward \
  --description "Add data quality fields"
```

## CLI Tools

### Schema Manager Commands

The `schema_manager.py` CLI provides comprehensive schema management:

```bash
# List all schemas
python scripts/schema_manager.py list

# Show schema details
python scripts/schema_manager.py show <schema_name> [--version VERSION] [--json]

# Compare versions
python scripts/schema_manager.py diff <schema_name> <from_version> <to_version>

# Validate schema
python scripts/schema_manager.py validate <schema_name> \
  --file <path> \
  [--strict] \
  [--compare-to VERSION] \
  [--strategy STRATEGY]

# Execute migration
python scripts/schema_manager.py migrate <schema_name> \
  --from VERSION \
  --to VERSION \
  [--dry-run] \
  [--yes]

# Register new version
python scripts/schema_manager.py register <schema_name> \
  --version VERSION \
  --file <path> \
  --strategy STRATEGY \
  [--description DESC]

# Rollback
python scripts/schema_manager.py rollback <schema_name> \
  --from VERSION \
  --to VERSION \
  [--dry-run] \
  [--yes]
```

### Output Formatting

The CLI uses colored output for clarity:

- ✅ Green: Success/compatible
- ❌ Red: Error/breaking
- ⚠️ Yellow: Warning
- ℹ️ Cyan: Information

## CI/CD Integration

### GitHub Actions Workflow

The schema validation workflow (`.github/workflows/schema_validation.yml`) automatically:

1. **Detects Schema Changes**: Identifies modified schema files in PRs
2. **Validates Definitions**: Checks schema syntax and structure
3. **Checks Compatibility**: Validates against compatibility strategy
4. **Shows Differences**: Displays changes in PR comments
5. **Registers Versions**: Auto-registers schemas on merge to main

### PR Validation Process

When you create a PR with schema changes:

1. Workflow detects changed schema files
2. Validates each schema definition
3. Checks compatibility with previous version
4. Posts results as PR comment
5. Blocks merge if breaking changes detected (without override)

### Triggering Migration Tests

Add `[schema-migration]` to your PR description to trigger migration dry-run tests:

```markdown
## Changes
- Added data quality fields to SDG schema

[schema-migration]
```

## Best Practices

### 1. Schema Design

✅ **DO**:
- Use descriptive column names
- Provide column descriptions
- Set appropriate nullable constraints
- Define defaults for new columns
- Use consistent naming conventions

❌ **DON'T**:
- Use reserved keywords as column names
- Mix case inconsistently
- Add required fields without defaults
- Use special characters in names

### 2. Version Management

✅ **DO**:
- Increment versions sequentially
- Document changes in metadata
- Use semantic compatibility strategies
- Test migrations with dry-run first
- Keep migration scripts in version control

❌ **DON'T**:
- Skip version numbers
- Reuse version numbers
- Make breaking changes without major version
- Delete old schema versions

### 3. Migration Safety

✅ **DO**:
- Always dry-run migrations first
- Backup data before migrations
- Use rollback capabilities
- Test on non-production first
- Monitor migration execution

❌ **DON'T**:
- Run migrations without dry-run
- Assume rollback will always work
- Apply untested migrations to production
- Ignore migration errors

### 4. Compatibility

✅ **DO**:
- Use BACKWARD compatibility by default
- Add columns as nullable initially
- Provide defaults for new required fields
- Plan for gradual deprecation

❌ **DON'T**:
- Change types without migration path
- Remove columns abruptly
- Add required fields without defaults
- Ignore compatibility warnings

## Examples

### Example 1: Adding Optional Fields

**Scenario**: Add data quality tracking fields to SDG schema

**v1 Schema**:
```python
SDG_SCHEMA_V1 = {
    "indicator_id": {"type": "String", "nullable": False},
    "country_id": {"type": "String", "nullable": False},
    "year": {"type": "Int", "nullable": False},
    "value": {"type": "Decimal", "nullable": True},
}
```

**v2 Schema** (add quality fields):
```python
SDG_SCHEMA_V2 = {
    **SDG_SCHEMA_V1,
    "data_source": {
        "type": "String",
        "nullable": True,
        "default": "UN",
        "description": "Source of the data"
    },
    "confidence_level": {
        "type": "Decimal",
        "nullable": True,
        "description": "Confidence level (0-1)"
    },
}
```

**Migration**:
```python
def create_v1_to_v2_migration():
    plan = MigrationPlan(schema_name="sdg.indicators", from_version=1, to_version=2)

    plan.add_operation(AddColumn(
        column_name="data_source",
        column_type="String",
        nullable=True,
        default="UN"
    ))

    plan.add_operation(AddColumn(
        column_name="confidence_level",
        column_type="Decimal",
        nullable=True
    ))

    return plan
```

**Compatibility**: ✅ BACKWARD (new schema reads old data)

### Example 2: Type Change with Migration

**Scenario**: Change year from Int to String for better formatting

**v2 Schema**:
```python
SDG_SCHEMA_V2 = {
    "year": {"type": "String", "nullable": False},
    # ... other fields
}
```

**Migration**:
```python
plan.add_operation(ChangeType(
    column_name="year",
    old_type="Int",
    new_type="String",
    using_expression="CAST(year AS VARCHAR)"
))
```

**Compatibility**: ❌ BREAKING (requires major version or NONE strategy)

### Example 3: Column Deprecation

**Scenario**: Remove deprecated "magnitude" field

**Step 1** (v2): Mark as deprecated, make nullable:
```python
SDG_SCHEMA_V2 = {
    # ... other fields
    "magnitude": {
        "type": "String",
        "nullable": True,
        "description": "DEPRECATED: Will be removed in v3"
    },
}
```

**Step 2** (v3): Remove column with backup:
```python
plan.add_operation(RemoveColumn(
    column_name="magnitude",
    backup_column=True  # Renames to magnitude_backup_<timestamp>
))
```

### Example 4: CLI Workflow

Complete workflow for adding a new schema version:

```bash
# 1. Create new schema version
cat > sqlMesh/schemas/sdg/v2.py << 'EOF'
from .v1 import SDG_SCHEMA_V1

SDG_SCHEMA_V2 = {
    **SDG_SCHEMA_V1,
    "data_source": {"type": "String", "nullable": True, "default": "UN"}
}
EOF

# 2. Validate schema
python scripts/schema_manager.py validate sdg.indicators \
  --file sqlMesh/schemas/sdg/v2.py \
  --compare-to 1 \
  --strategy backward

# 3. Register new version
python scripts/schema_manager.py register sdg.indicators \
  --version 2 \
  --file sqlMesh/schemas/sdg/v2.py \
  --strategy backward \
  --description "Add data source tracking"

# 4. Test migration
python scripts/schema_manager.py migrate sdg.indicators \
  --from 1 --to 2 --dry-run

# 5. Execute migration
python scripts/schema_manager.py migrate sdg.indicators \
  --from 1 --to 2

# 6. Verify
python scripts/schema_manager.py show sdg.indicators --version 2
```

### Example 5: Handling Breaking Changes

**Scenario**: Need to make breaking changes

```bash
# Option 1: Use NONE strategy for breaking changes
python scripts/schema_manager.py register sdg.indicators \
  --version 3 \
  --file sqlMesh/schemas/sdg/v3.py \
  --strategy none \
  --description "BREAKING: Major schema refactoring"

# Option 2: Create migration path with intermediate version
# v2: Add new fields (backward compatible)
# v3: Deprecate old fields (backward compatible)
# v4: Remove old fields (breaking, but data migrated)
```

## Troubleshooting

### Common Issues

#### Issue: "Schema validation failed"

**Cause**: Schema definition doesn't meet validation rules

**Solution**: Check validation errors and fix:
```bash
python scripts/schema_manager.py validate sdg.indicators \
  --file sqlMesh/schemas/sdg/v2.py \
  --strict
```

#### Issue: "Breaking changes detected"

**Cause**: Changes are incompatible with current strategy

**Solution**:
1. Review compatibility strategy
2. Adjust changes to be compatible, or
3. Use NONE strategy for intentional breaking changes

#### Issue: "Migration failed"

**Cause**: SQL execution error or data incompatibility

**Solution**:
1. Check error message
2. Test with dry-run first
3. Verify data compatibility
4. Rollback if needed:
```bash
python scripts/schema_manager.py rollback sdg.indicators \
  --from 2 --to 1
```

#### Issue: "Version already exists"

**Cause**: Trying to register duplicate version

**Solution**: Use next version number or update existing version definition

## Advanced Topics

### Custom Compatibility Rules

For custom compatibility rules, extend the `SchemaValidator` class:

```python
from src.pipeline.schema_validator import SchemaValidator

class CustomValidator(SchemaValidator):
    def _check_custom_rule(self, old_schema, new_schema):
        # Custom validation logic
        pass
```

### Integration with Data Catalog

The schema registry can integrate with data catalogs:

```python
from src.pipeline.schema_registry import SchemaRegistry

registry = SchemaRegistry()
schema = registry.get_latest_schema("sdg.indicators")

# Export to catalog format
catalog_entry = {
    "name": schema.schema_name,
    "version": schema.version,
    "columns": schema.schema_definition,
    "metadata": schema.metadata
}
```

### Programmatic Usage

Use the schema system programmatically:

```python
from src.pipeline.schema_registry import SchemaRegistry, CompatibilityStrategy
from src.pipeline.schema_migration import SchemaMigration

# Initialize
registry = SchemaRegistry()
migrator = SchemaMigration("sdg__indicators")

# Register schema
registry.register_schema(
    schema_name="sdg.indicators",
    version=2,
    schema_definition=SDG_SCHEMA_V2,
    compatibility_strategy=CompatibilityStrategy.BACKWARD
)

# Create and execute migration
plan = migrator.create_migration_plan("sdg.indicators", from_version=1, to_version=2)
migrator.execute_plan(plan, dry_run=False)
```

## References

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Schema Evolution Patterns](https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protobuf-thrift.html)
- [Data Model Versioning Best Practices](https://www.confluent.io/blog/schema-registry-avro-in-depth/)

## Support

For issues or questions:

1. Check this documentation
2. Review examples in `sqlMesh/schemas/`
3. Run CLI with `--help` flag
4. Check GitHub workflow logs
5. Contact the data engineering team
