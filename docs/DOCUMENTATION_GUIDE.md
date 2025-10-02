# OSAA Data Pipeline - Documentation Maintenance Guide

## Table of Contents

1. [Overview](#overview)
2. [Documentation Structure](#documentation-structure)
3. [Maintaining Model Documentation](#maintaining-model-documentation)
4. [Catalog Generation Process](#catalog-generation-process)
5. [Documentation Standards](#documentation-standards)
6. [Best Practices](#best-practices)
7. [Troubleshooting](#troubleshooting)
8. [Review Checklist](#review-checklist)

## Overview

This guide provides instructions for maintaining and updating the OSAA Data Pipeline documentation. Following these guidelines ensures consistency, accuracy, and completeness across all documentation.

### Documentation Philosophy

- **Documentation as Code**: Treat documentation with the same rigor as code
- **Single Source of Truth**: Model docstrings are the primary source
- **Automation First**: Use automated generation wherever possible
- **User-Focused**: Write for your audience (developers, analysts, operators)
- **Living Documentation**: Keep documentation current with code changes

## Documentation Structure

### Documentation Hierarchy

```
osaa-mvp/
├── README.md                    # Project overview and quickstart
├── docs/
│   ├── DATA_CATALOG.md         # Auto-generated data catalog
│   ├── catalog.json             # Machine-readable catalog
│   ├── catalog.html             # Interactive web catalog
│   ├── OPERATIONAL_RUNBOOK.md  # Operations procedures
│   ├── API_REFERENCE.md        # API documentation
│   ├── BUSINESS_GLOSSARY.md    # Business terminology
│   ├── DOCUMENTATION_GUIDE.md  # This file
│   └── lineage/                 # Data lineage diagrams
│       ├── sdg_lineage.mmd
│       ├── wdi_lineage.mmd
│       ├── opri_lineage.mmd
│       └── full_pipeline.mmd
├── sqlMesh/models/              # Model files with embedded docs
└── scripts/
    └── generate_data_catalog.py # Catalog generation script
```

### Document Types

| Document | Type | Update Method | Frequency |
|----------|------|---------------|-----------|
| Model Docstrings | Manual | Direct edit | With code changes |
| DATA_CATALOG.md | Auto | Script generation | Daily/On change |
| catalog.json | Auto | Script generation | Daily/On change |
| catalog.html | Auto | Script generation | Daily/On change |
| OPERATIONAL_RUNBOOK.md | Manual | Direct edit | Quarterly |
| API_REFERENCE.md | Manual | Direct edit | With API changes |
| BUSINESS_GLOSSARY.md | Manual | Direct edit | As needed |
| Lineage Diagrams | Manual | Direct edit | With flow changes |

## Maintaining Model Documentation

### Model Docstring Template

Every SQLMesh model MUST include comprehensive documentation in its docstring:

```python
@model(
    "sources.example",
    is_sql=True,
    kind="FULL",
    columns=COLUMN_SCHEMA,
    description="Brief one-line description",
    column_descriptions={
        "column1": "Description of column1",
        "column2": "Description of column2",
    },
    grain=("key1", "key2"),
    physical_properties={
        "publishing_org": "Organization name",
        "link_to_raw_data": "https://...",
        "dataset_owner": "Owner name",
        "update_cadence": "Daily/Weekly/Monthly",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    """
    Model Name - Descriptive Title

    Comprehensive description of what this model does, its purpose,
    and how it fits into the overall pipeline.

    Business Context:
        - Why this data is important
        - Who uses it
        - Key business questions it answers
        - Related processes and decisions

    Data Quality Standards:
        - Completeness: Target percentage
        - Timeliness: Update frequency and lag
        - Accuracy: Validation methods
        - Known Issues:
            * Issue 1 description
            * Issue 2 description

    Column Details:
        - column1 (Type, NULLABLE/NOT NULL): Detailed description
          including business meaning, valid values, relationships
        - column2 (Type, NULLABLE/NOT NULL): Description

    Data Processing:
        1. Step 1 description
        2. Step 2 description
        3. Step 3 description

    Usage Examples:
        -- Example query 1
        SELECT * FROM sources.example
        WHERE condition = value;

        -- Example query 2
        SELECT column1, COUNT(*)
        FROM sources.example
        GROUP BY column1;

    Dependencies:
        Upstream:
            - s3://path/to/source/data
            - model.dependency1
        Downstream:
            - model.consumer1
            - dashboard.example

    Update Frequency: Daily/Weekly/Monthly
    SLA: XX hours from source availability
    Owner: Team/Person name
    Contact: email@example.org
    Last Updated: YYYY-MM-DD
    Version: X.Y.Z

    Change Log:
        - YYYY-MM-DD: Change description
        - YYYY-MM-DD: Initial creation
    """
    # Model implementation
    pass
```

### Column Documentation Standards

#### Required Information

For each column, document:

1. **Name**: Use descriptive, consistent naming
2. **Type**: Specify data type (String, Int, Decimal, etc.)
3. **Nullable**: Explicitly state if NULL values allowed
4. **Description**: Clear, concise explanation
5. **Business Meaning**: How business users interpret this
6. **Valid Values**: Constraints, ranges, or enumerations
7. **Examples**: 2-3 example values

#### Column Description Format

```python
column_descriptions={
    "indicator_id": "Unique identifier for the metric (e.g., '1.1.1' for SDG poverty indicator)",
    "country_id": "ISO 3166-1 alpha-3 country code (e.g., 'USA', 'GBR', 'KEN')",
    "year": "Reference year for the data point (1990-present)",
    "value": "Measured value (units vary by indicator, see magnitude field)",
}
```

### Adding New Models

When creating a new model:

1. **Copy Template**: Start with the docstring template above
2. **Fill All Sections**: Don't leave any section empty
3. **Add Column Schema**: Define COLUMN_SCHEMA dictionary
4. **Document Columns**: Add column_descriptions to @model decorator
5. **Include Examples**: Provide 2-3 realistic usage examples
6. **Specify Dependencies**: List all upstream and downstream dependencies
7. **Run Catalog Generation**: Test that your documentation generates correctly

## Catalog Generation Process

### Manual Generation

```bash
# Generate catalog locally
cd /path/to/osaa-mvp
python scripts/generate_data_catalog.py

# Check generated files
ls -la docs/*.md docs/*.json docs/*.html
```

### Automated Generation

The catalog is automatically generated via GitHub Actions:

- **On Push**: When models or script change on main branch
- **On PR**: Generates catalog for review
- **Daily**: Scheduled run at 2 AM UTC
- **Manual**: Trigger via GitHub Actions UI

### Validation Steps

1. **Check Script Execution**:
   ```bash
   python scripts/generate_data_catalog.py
   # Should complete without errors
   ```

2. **Validate JSON**:
   ```bash
   python -c "import json; json.load(open('docs/catalog.json'))"
   ```

3. **Check HTML**:
   ```bash
   # Open in browser
   open docs/catalog.html
   ```

4. **Review Markdown**:
   ```bash
   # Check for completeness
   grep -c "^###" docs/DATA_CATALOG.md
   ```

## Documentation Standards

### Writing Style

#### Clarity
- Use clear, simple language
- Avoid jargon without explanation
- Define acronyms on first use
- Use active voice

#### Consistency
- Use consistent terminology
- Follow naming conventions
- Maintain uniform formatting
- Use standard date format (YYYY-MM-DD)

#### Completeness
- Document all public interfaces
- Include examples for complex concepts
- Provide context and rationale
- Link to related documentation

### Formatting Guidelines

#### Markdown Standards

```markdown
# Level 1 Heading - Document Title
## Level 2 Heading - Major Sections
### Level 3 Heading - Subsections
#### Level 4 Heading - Details

**Bold** for emphasis
*Italic* for first use of terms
`code` for inline code
```

#### Code Blocks

````markdown
```python
# Python code with syntax highlighting
def example():
    pass
```

```sql
-- SQL queries with syntax highlighting
SELECT * FROM table;
```
````

#### Tables

```markdown
| Column | Type | Description |
|--------|------|-------------|
| id | Int | Unique ID |
| name | String | Name field |
```

### Version Control

#### Commit Messages

When updating documentation:

```bash
# For documentation-only changes
git commit -m "docs: Update model documentation for SDG indicators"

# For code + documentation
git commit -m "feat: Add new WDI model with comprehensive docs"

# For fixes
git commit -m "fix: Correct column descriptions in OPRI model"
```

#### Pull Request Template

```markdown
## Documentation Updates

### Changes Made
- [ ] Updated model docstrings
- [ ] Regenerated data catalog
- [ ] Updated operational runbook
- [ ] Added/updated examples
- [ ] Reviewed for accuracy

### Validation
- [ ] Catalog generation successful
- [ ] JSON validation passed
- [ ] HTML catalog renders correctly
- [ ] No broken links

### Review Notes
[Any specific areas needing review]
```

## Best Practices

### Do's

✅ **DO** update documentation with every code change
✅ **DO** run catalog generation after model changes
✅ **DO** include real-world examples
✅ **DO** explain the "why" not just the "what"
✅ **DO** use diagrams for complex relationships
✅ **DO** test documentation accuracy
✅ **DO** review documentation in PRs
✅ **DO** keep language user-appropriate

### Don'ts

❌ **DON'T** leave placeholder text
❌ **DON'T** document obvious things
❌ **DON'T** use unexplained acronyms
❌ **DON'T** copy-paste without updating
❌ **DON'T** ignore documentation in reviews
❌ **DON'T** let documentation become stale
❌ **DON'T** mix concerns (technical/business)

### Documentation Review Process

1. **Self-Review**:
   - Read your documentation as a new user
   - Check for completeness and clarity
   - Verify examples work
   - Run spell check

2. **Peer Review**:
   - Have another team member review
   - Check technical accuracy
   - Validate business context
   - Ensure consistency

3. **Stakeholder Review**:
   - Business users review glossary
   - Operators review runbook
   - Developers review API docs
   - Analysts review catalog

## Troubleshooting

### Common Issues

#### Issue: Catalog Generation Fails

```bash
# Check Python environment
python --version  # Should be 3.8+

# Install dependencies
pip install -r requirements.txt

# Check model syntax
python -m py_compile sqlMesh/models/sources/*.py
```

#### Issue: Documentation Not Updating

```bash
# Force regeneration
rm docs/DATA_CATALOG.md docs/catalog.json docs/catalog.html
python scripts/generate_data_catalog.py

# Check git status
git status docs/
```

#### Issue: Malformed Docstrings

```python
# Validate Python syntax
import ast
with open('model.py') as f:
    ast.parse(f.read())
```

#### Issue: Missing Metadata

```python
# Check model decorator
@model(
    # Ensure all required fields present
    description="...",  # Required
    column_descriptions={...},  # Required
    physical_properties={...},  # Recommended
)
```

### Debugging Tips

1. **Enable Verbose Logging**:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Test Individual Models**:
   ```python
   from scripts.generate_data_catalog import DataCatalogGenerator
   gen = DataCatalogGenerator('.')
   metadata = gen._extract_model_metadata(Path('model.py'))
   print(metadata)
   ```

3. **Validate JSON Output**:
   ```bash
   cat docs/catalog.json | python -m json.tool
   ```

## Review Checklist

### Before Committing

- [ ] Model docstring is complete
- [ ] All columns documented
- [ ] Examples are working
- [ ] Dependencies listed
- [ ] Catalog generates successfully
- [ ] No spelling/grammar errors
- [ ] Formatting is consistent
- [ ] Links are valid

### During PR Review

- [ ] Documentation matches code changes
- [ ] Business context is clear
- [ ] Technical details are accurate
- [ ] Examples demonstrate usage
- [ ] No sensitive information exposed
- [ ] Version/date updated
- [ ] Change log entry added

### Post-Deployment

- [ ] Catalog accessible in production
- [ ] Documentation linked from README
- [ ] Stakeholders notified of changes
- [ ] Feedback incorporated
- [ ] Issues/gaps tracked

## Documentation Templates

### New Data Source Template

```markdown
## [Source Name] Data

### Overview
Brief description of the data source and its purpose.

### Source System
- **Provider**: Organization name
- **URL**: https://...
- **Update Frequency**: Daily/Weekly/Monthly
- **Data Format**: CSV/JSON/API

### Data Elements
Description of key data elements and relationships.

### Integration Method
How data is ingested and processed.

### Usage
Common use cases and queries.

### Quality Notes
Known issues, limitations, or considerations.

### Contact
- Owner: name@example.org
- Technical: tech@example.org
```

### Operational Procedure Template

```markdown
## [Procedure Name]

### Purpose
What this procedure accomplishes.

### Prerequisites
- Required access/permissions
- Tools needed
- Initial conditions

### Steps
1. **Step Name**
   ```bash
   command to execute
   ```
   Expected output or result

2. **Next Step**
   Description and commands

### Validation
How to verify success.

### Rollback
Steps if procedure fails.

### Notes
Additional considerations or tips.
```

## Appendix

### Useful Tools

| Tool | Purpose | Usage |
|------|---------|-------|
| [Mermaid Live Editor](https://mermaid.live) | Create lineage diagrams | Design and test diagrams |
| [Markdown Preview](https://markdownlivepreview.com) | Preview Markdown | Check formatting |
| [JSON Validator](https://jsonlint.com) | Validate JSON | Check catalog.json |
| [Grammarly](https://grammarly.com) | Grammar check | Review documentation |

### Documentation Resources

- [SQLMesh Documentation](https://sqlmesh.readthedocs.io/)
- [Markdown Guide](https://www.markdownguide.org/)
- [Mermaid Diagram Syntax](https://mermaid-js.github.io/)
- [Google Developer Documentation Style Guide](https://developers.google.com/style)

### Contact for Help

- **Documentation Questions**: stephen.sciortino@un.org
- **Technical Issues**: Create GitHub issue
- **Process Improvements**: Submit PR with suggestions

---

*Last Updated: 2025-10-02*
*Version: 1.0.0*