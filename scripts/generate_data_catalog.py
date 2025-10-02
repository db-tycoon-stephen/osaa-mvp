#!/usr/bin/env python3
"""
Data Catalog Generator for OSAA Data Pipeline

This script automatically generates comprehensive data documentation by:
1. Scanning SQLMesh models for metadata
2. Extracting column definitions and relationships
3. Building data lineage graphs
4. Generating catalogs in multiple formats (Markdown, JSON, HTML)

Author: UN-OSAA Data Team
Date: 2025-10-02
"""

import json
import os
import sys
import re
import ast
import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ColumnMetadata:
    """Represents metadata for a table column."""
    name: str
    type: str
    nullable: bool = True
    description: str = ""
    business_meaning: str = ""
    validation_rules: str = ""
    example_values: List[str] = None

    def __post_init__(self):
        if self.example_values is None:
            self.example_values = []

@dataclass
class TableMetadata:
    """Represents metadata for a database table."""
    name: str
    schema: str
    description: str
    columns: List[ColumnMetadata]
    grain: List[str] = None
    update_frequency: str = "Daily"
    sla_hours: int = 24
    owner: str = "UN-OSAA Data Team"
    upstream_dependencies: List[str] = None
    downstream_dependencies: List[str] = None
    sample_queries: List[str] = None
    physical_properties: Dict[str, str] = None

    def __post_init__(self):
        if self.grain is None:
            self.grain = []
        if self.upstream_dependencies is None:
            self.upstream_dependencies = []
        if self.downstream_dependencies is None:
            self.downstream_dependencies = []
        if self.sample_queries is None:
            self.sample_queries = []
        if self.physical_properties is None:
            self.physical_properties = {}


class DataCatalogGenerator:
    """Main class for generating data catalog documentation."""

    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.models_dir = self.project_root / "sqlMesh" / "models"
        self.docs_dir = self.project_root / "docs"
        self.catalog_data: Dict[str, TableMetadata] = {}
        self.lineage_graph: Dict[str, Dict[str, List[str]]] = {}

    def scan_models(self) -> None:
        """Scan all SQLMesh model files and extract metadata."""
        logger.info("Scanning SQLMesh models...")

        # Find all Python model files
        model_files = list(self.models_dir.glob("**/*.py"))

        for model_file in model_files:
            if model_file.name.startswith("_") or model_file.name == "__init__.py":
                continue

            try:
                metadata = self._extract_model_metadata(model_file)
                if metadata:
                    full_name = f"{metadata.schema}.{metadata.name}"
                    self.catalog_data[full_name] = metadata
                    logger.info(f"Extracted metadata for {full_name}")
            except Exception as e:
                logger.warning(f"Failed to extract metadata from {model_file}: {e}")

    def _extract_model_metadata(self, model_path: Path) -> Optional[TableMetadata]:
        """Extract metadata from a single model file."""
        with open(model_path, 'r') as f:
            content = f.read()

        # Parse Python AST to extract information
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return None

        # Find model decorator and extract metadata
        metadata = None
        column_schema = {}
        column_descriptions = {}
        physical_properties = {}
        grain = []

        for node in ast.walk(tree):
            # Find COLUMN_SCHEMA definition
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "COLUMN_SCHEMA":
                        if isinstance(node.value, ast.Dict):
                            column_schema = self._parse_dict_literal(node.value)

            # Find @model decorator
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Call):
                        if hasattr(decorator.func, 'id') and decorator.func.id == 'model':
                            # Extract model parameters
                            model_params = self._parse_model_decorator(decorator)

                            # Extract schema and table name
                            if model_params.get('model_name'):
                                parts = model_params['model_name'].strip('"').split('.')
                                schema = parts[0] if len(parts) > 1 else 'default'
                                table = parts[-1]

                                # Extract column descriptions
                                column_descriptions = model_params.get('column_descriptions', {})
                                physical_properties = model_params.get('physical_properties', {})
                                grain = model_params.get('grain', [])

                                # Create metadata object
                                metadata = self._create_table_metadata(
                                    schema=schema,
                                    table=table,
                                    column_schema=column_schema,
                                    column_descriptions=column_descriptions,
                                    description=model_params.get('description', ''),
                                    physical_properties=physical_properties,
                                    grain=grain,
                                    model_path=model_path
                                )

        return metadata

    def _parse_dict_literal(self, node: ast.Dict) -> Dict:
        """Parse an AST dictionary literal into a Python dict."""
        result = {}
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant):
                key_str = key.value
            elif isinstance(key, ast.Str):
                key_str = key.s
            else:
                continue

            if isinstance(value, ast.Constant):
                result[key_str] = value.value
            elif isinstance(value, ast.Str):
                result[key_str] = value.s
            elif isinstance(value, ast.Dict):
                result[key_str] = self._parse_dict_literal(value)
        return result

    def _parse_model_decorator(self, decorator: ast.Call) -> Dict:
        """Parse @model decorator arguments."""
        params = {}

        # Parse positional arguments
        if decorator.args:
            if isinstance(decorator.args[0], ast.Constant):
                params['model_name'] = decorator.args[0].value
            elif isinstance(decorator.args[0], ast.Str):
                params['model_name'] = decorator.args[0].s

        # Parse keyword arguments
        for keyword in decorator.keywords:
            key = keyword.arg
            value = keyword.value

            if isinstance(value, ast.Constant):
                params[key] = value.value
            elif isinstance(value, ast.Str):
                params[key] = value.s
            elif isinstance(value, ast.Dict):
                params[key] = self._parse_dict_literal(value)
            elif isinstance(value, (ast.List, ast.Tuple)):
                params[key] = self._parse_list_literal(value)

        return params

    def _parse_list_literal(self, node) -> List:
        """Parse an AST list/tuple literal into a Python list."""
        result = []
        for elem in node.elts:
            if isinstance(elem, ast.Constant):
                result.append(elem.value)
            elif isinstance(elem, ast.Str):
                result.append(elem.s)
        return result

    def _create_table_metadata(
        self,
        schema: str,
        table: str,
        column_schema: Dict,
        column_descriptions: Dict,
        description: str,
        physical_properties: Dict,
        grain: List,
        model_path: Path
    ) -> TableMetadata:
        """Create TableMetadata object from extracted information."""

        # Build column metadata
        columns = []
        for col_name, col_type in column_schema.items():
            col_meta = ColumnMetadata(
                name=col_name,
                type=col_type,
                description=column_descriptions.get(col_name, ""),
                business_meaning=self._get_business_meaning(col_name),
                example_values=self._get_example_values(col_name)
            )
            columns.append(col_meta)

        # Extract dependencies from model file
        upstream, downstream = self._extract_dependencies(model_path)

        # Create table metadata
        table_meta = TableMetadata(
            name=table,
            schema=schema,
            description=description or f"Table containing {table} data",
            columns=columns,
            grain=grain if isinstance(grain, list) else [grain] if grain else [],
            update_frequency=physical_properties.get('update_cadence', 'Daily'),
            owner=physical_properties.get('dataset_owner', 'UN-OSAA Data Team'),
            upstream_dependencies=upstream,
            downstream_dependencies=downstream,
            sample_queries=self._generate_sample_queries(schema, table, columns),
            physical_properties=physical_properties
        )

        return table_meta

    def _get_business_meaning(self, column_name: str) -> str:
        """Get business meaning for common column names."""
        meanings = {
            "indicator_id": "Unique identifier for the indicator metric being measured",
            "country_id": "ISO 3166-1 alpha-3 country code for geographic identification",
            "year": "The calendar year for which the data point is recorded",
            "value": "The numeric measurement or observation for the indicator",
            "magnitude": "Scale or order of magnitude for the value (e.g., thousands, millions)",
            "qualifier": "Additional context or quality notes about the data point",
            "indicator_description": "Human-readable description of what the indicator measures",
            "source": "The originating dataset or system that provided this data"
        }
        return meanings.get(column_name, "")

    def _get_example_values(self, column_name: str) -> List[str]:
        """Get example values for common column names."""
        examples = {
            "indicator_id": ["1.1.1", "2.3.2", "17.18.1", "SDG_1_1_1"],
            "country_id": ["USA", "GBR", "JPN", "BRA", "IND"],
            "year": ["2020", "2021", "2022", "2023", "2024"],
            "magnitude": ["units", "thousands", "millions", "percentage"],
            "qualifier": ["estimated", "provisional", "final", "revised"],
            "source": ["sdg", "wdi", "opri", "edu"]
        }
        return examples.get(column_name, [])

    def _extract_dependencies(self, model_path: Path) -> Tuple[List[str], List[str]]:
        """Extract upstream and downstream dependencies from model file."""
        upstream = []
        downstream = []

        with open(model_path, 'r') as f:
            content = f.read()

        # Look for table references
        # Pattern for generate_ibis_table calls
        table_pattern = r'generate_ibis_table\([^)]*table_name="([^"]+)"[^)]*schema_name="([^"]+)"'
        for match in re.finditer(table_pattern, content):
            table_name = match.group(1)
            schema_name = match.group(2)
            upstream.append(f"{schema_name}.{table_name}")

        # Pattern for S3 reads
        s3_pattern = r'@s3_read\([\'"]([^\'"]+)[\'"]\)'
        for match in re.finditer(s3_pattern, content):
            upstream.append(f"s3://{match.group(1)}")

        # Pattern for read_parquet
        parquet_pattern = r'read_parquet\([^)]*[\'"]([^\'"]+)[\'"]\)'
        for match in re.finditer(parquet_pattern, content):
            upstream.append(f"file://{match.group(1)}")

        # Downstream dependencies would be found by scanning other models
        # For now, we'll add known downstream patterns
        if "sources" in str(model_path):
            downstream.append("master.indicators")

        return upstream, downstream

    def _generate_sample_queries(self, schema: str, table: str, columns: List[ColumnMetadata]) -> List[str]:
        """Generate sample SQL queries for the table."""
        queries = []

        # Basic select all
        queries.append(f"-- Select all records\nSELECT * FROM {schema}.{table} LIMIT 10;")

        # If has year column, get latest data
        if any(col.name == "year" for col in columns):
            queries.append(
                f"-- Get latest year data\n"
                f"SELECT * FROM {schema}.{table}\n"
                f"WHERE year = (SELECT MAX(year) FROM {schema}.{table});"
            )

        # If has country_id, filter by country
        if any(col.name == "country_id" for col in columns):
            queries.append(
                f"-- Get data for specific country\n"
                f"SELECT * FROM {schema}.{table}\n"
                f"WHERE country_id = 'USA';"
            )

        # If has indicator_id, filter by indicator
        if any(col.name == "indicator_id" for col in columns):
            queries.append(
                f"-- Get specific indicator data\n"
                f"SELECT * FROM {schema}.{table}\n"
                f"WHERE indicator_id LIKE '1.%';"
            )

        return queries

    def build_lineage(self) -> None:
        """Build data lineage graph from dependencies."""
        logger.info("Building data lineage...")

        for table_name, metadata in self.catalog_data.items():
            if table_name not in self.lineage_graph:
                self.lineage_graph[table_name] = {
                    "upstream": [],
                    "downstream": []
                }

            self.lineage_graph[table_name]["upstream"] = metadata.upstream_dependencies

            # Update downstream dependencies for upstream tables
            for upstream in metadata.upstream_dependencies:
                if upstream not in self.lineage_graph:
                    self.lineage_graph[upstream] = {
                        "upstream": [],
                        "downstream": []
                    }
                if table_name not in self.lineage_graph[upstream]["downstream"]:
                    self.lineage_graph[upstream]["downstream"].append(table_name)

    def generate_markdown_catalog(self) -> str:
        """Generate Markdown format catalog."""
        logger.info("Generating Markdown catalog...")

        md_lines = []

        # Header
        md_lines.append("# OSAA Data Pipeline - Data Catalog")
        md_lines.append("")
        md_lines.append(f"*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        md_lines.append("")

        # Executive Summary
        md_lines.append("## Executive Summary")
        md_lines.append("")
        md_lines.append("The OSAA Data Pipeline processes and integrates multiple data sources to support ")
        md_lines.append("sustainable development monitoring and analysis. This catalog documents all ")
        md_lines.append("datasets, their structures, relationships, and usage patterns.")
        md_lines.append("")
        md_lines.append(f"- **Total Datasets**: {len(self.catalog_data)}")
        md_lines.append(f"- **Data Sources**: SDG (UN), WDI (World Bank), OPRI, EDU")
        md_lines.append("- **Update Frequency**: Daily")
        md_lines.append("- **Primary Use Cases**: SDG monitoring, development indicators analysis, policy research")
        md_lines.append("")

        # Table of Contents
        md_lines.append("## Table of Contents")
        md_lines.append("")
        md_lines.append("1. [Dataset Inventory](#dataset-inventory)")
        md_lines.append("2. [Column Dictionary](#column-dictionary)")
        md_lines.append("3. [Data Lineage](#data-lineage)")
        md_lines.append("4. [Usage Examples](#usage-examples)")
        md_lines.append("5. [Data Quality Standards](#data-quality-standards)")
        md_lines.append("6. [Contact Information](#contact-information)")
        md_lines.append("")

        # Dataset Inventory
        md_lines.append("## Dataset Inventory")
        md_lines.append("")

        for table_name in sorted(self.catalog_data.keys()):
            metadata = self.catalog_data[table_name]
            md_lines.append(f"### {table_name}")
            md_lines.append("")
            md_lines.append(f"**Description**: {metadata.description}")
            md_lines.append("")

            # Table metadata
            md_lines.append("| Property | Value |")
            md_lines.append("|----------|--------|")
            md_lines.append(f"| Schema | `{metadata.schema}` |")
            md_lines.append(f"| Table | `{metadata.name}` |")
            md_lines.append(f"| Update Frequency | {metadata.update_frequency} |")
            md_lines.append(f"| SLA | {metadata.sla_hours} hours |")
            md_lines.append(f"| Owner | {metadata.owner} |")
            if metadata.grain:
                md_lines.append(f"| Grain | {', '.join(metadata.grain)} |")
            md_lines.append("")

            # Physical properties if available
            if metadata.physical_properties:
                md_lines.append("**Data Source Information**:")
                md_lines.append("")
                for key, value in metadata.physical_properties.items():
                    if value:
                        formatted_key = key.replace('_', ' ').title()
                        md_lines.append(f"- **{formatted_key}**: {value}")
                md_lines.append("")

            # Columns
            md_lines.append("**Columns**:")
            md_lines.append("")
            md_lines.append("| Column | Type | Nullable | Description |")
            md_lines.append("|--------|------|----------|-------------|")

            for col in metadata.columns:
                nullable = "Yes" if col.nullable else "No"
                md_lines.append(f"| `{col.name}` | {col.type} | {nullable} | {col.description} |")
            md_lines.append("")

            # Dependencies
            if metadata.upstream_dependencies:
                md_lines.append("**Upstream Dependencies**:")
                md_lines.append("")
                for dep in metadata.upstream_dependencies:
                    md_lines.append(f"- `{dep}`")
                md_lines.append("")

            if metadata.downstream_dependencies:
                md_lines.append("**Downstream Dependencies**:")
                md_lines.append("")
                for dep in metadata.downstream_dependencies:
                    md_lines.append(f"- `{dep}`")
                md_lines.append("")

            # Sample queries
            if metadata.sample_queries:
                md_lines.append("**Sample Queries**:")
                md_lines.append("")
                for query in metadata.sample_queries:
                    md_lines.append("```sql")
                    md_lines.append(query)
                    md_lines.append("```")
                    md_lines.append("")

            md_lines.append("---")
            md_lines.append("")

        # Column Dictionary
        md_lines.append("## Column Dictionary")
        md_lines.append("")
        md_lines.append("This section provides detailed information about all columns across all datasets.")
        md_lines.append("")

        # Collect all unique columns
        all_columns = {}
        for metadata in self.catalog_data.values():
            for col in metadata.columns:
                if col.name not in all_columns:
                    all_columns[col.name] = col

        for col_name in sorted(all_columns.keys()):
            col = all_columns[col_name]
            md_lines.append(f"### {col_name}")
            md_lines.append("")
            md_lines.append(f"- **Type**: `{col.type}`")
            md_lines.append(f"- **Description**: {col.description}")
            if col.business_meaning:
                md_lines.append(f"- **Business Meaning**: {col.business_meaning}")
            if col.example_values:
                md_lines.append(f"- **Example Values**: {', '.join([f'`{v}`' for v in col.example_values[:5]])}")
            md_lines.append("")

        # Data Lineage
        md_lines.append("## Data Lineage")
        md_lines.append("")
        md_lines.append("### Overall Pipeline Flow")
        md_lines.append("")
        md_lines.append("```mermaid")
        md_lines.append("graph LR")
        md_lines.append("    subgraph Sources")
        md_lines.append("        S1[S3: Raw CSV Files]")
        md_lines.append("        S2[API: External Data]")
        md_lines.append("    end")
        md_lines.append("    ")
        md_lines.append("    subgraph Landing")
        md_lines.append("        L1[SDG Data]")
        md_lines.append("        L2[WDI Data]")
        md_lines.append("        L3[OPRI Data]")
        md_lines.append("    end")
        md_lines.append("    ")
        md_lines.append("    subgraph Transform")
        md_lines.append("        T1[sources.sdg]")
        md_lines.append("        T2[sources.wdi]")
        md_lines.append("        T3[sources.opri]")
        md_lines.append("    end")
        md_lines.append("    ")
        md_lines.append("    subgraph Master")
        md_lines.append("        M1[master.indicators]")
        md_lines.append("    end")
        md_lines.append("    ")
        md_lines.append("    subgraph Output")
        md_lines.append("        O1[S3: Final Data]")
        md_lines.append("    end")
        md_lines.append("    ")
        md_lines.append("    S1 --> L1")
        md_lines.append("    S1 --> L2")
        md_lines.append("    S1 --> L3")
        md_lines.append("    S2 --> L2")
        md_lines.append("    ")
        md_lines.append("    L1 --> T1")
        md_lines.append("    L2 --> T2")
        md_lines.append("    L3 --> T3")
        md_lines.append("    ")
        md_lines.append("    T1 --> M1")
        md_lines.append("    T2 --> M1")
        md_lines.append("    T3 --> M1")
        md_lines.append("    ")
        md_lines.append("    M1 --> O1")
        md_lines.append("```")
        md_lines.append("")

        # Usage Examples
        md_lines.append("## Usage Examples")
        md_lines.append("")
        md_lines.append("### Example 1: Get Latest SDG Data for All Countries")
        md_lines.append("")
        md_lines.append("```sql")
        md_lines.append("SELECT ")
        md_lines.append("    country_id,")
        md_lines.append("    indicator_id,")
        md_lines.append("    year,")
        md_lines.append("    value,")
        md_lines.append("    indicator_description")
        md_lines.append("FROM sources.sdg")
        md_lines.append("WHERE year = (SELECT MAX(year) FROM sources.sdg)")
        md_lines.append("ORDER BY country_id, indicator_id;")
        md_lines.append("```")
        md_lines.append("")

        md_lines.append("### Example 2: Compare Indicators Across Sources")
        md_lines.append("")
        md_lines.append("```sql")
        md_lines.append("SELECT ")
        md_lines.append("    source,")
        md_lines.append("    indicator_id,")
        md_lines.append("    country_id,")
        md_lines.append("    year,")
        md_lines.append("    value")
        md_lines.append("FROM master.indicators")
        md_lines.append("WHERE country_id = 'USA'")
        md_lines.append("  AND year >= 2020")
        md_lines.append("ORDER BY indicator_id, year;")
        md_lines.append("```")
        md_lines.append("")

        # Data Quality Standards
        md_lines.append("## Data Quality Standards")
        md_lines.append("")
        md_lines.append("### Completeness")
        md_lines.append("- **Target**: >95% non-null values for required fields")
        md_lines.append("- **Monitoring**: Daily quality checks via automated pipeline")
        md_lines.append("")

        md_lines.append("### Timeliness")
        md_lines.append("- **SLA**: 24 hours from source update to pipeline completion")
        md_lines.append("- **Update Schedule**: Daily at 02:00 UTC")
        md_lines.append("")

        md_lines.append("### Accuracy")
        md_lines.append("- **Validation**: Schema validation on ingestion")
        md_lines.append("- **Cross-checks**: Comparison with source systems")
        md_lines.append("")

        md_lines.append("### Consistency")
        md_lines.append("- **Standards**: ISO country codes, UN indicator definitions")
        md_lines.append("- **Formats**: Standardized date formats (YYYY-MM-DD)")
        md_lines.append("")

        # Contact Information
        md_lines.append("## Contact Information")
        md_lines.append("")
        md_lines.append("**Data Team Contacts**:")
        md_lines.append("")
        md_lines.append("- **Project Sponsor**: Mirian Lima - mirian.lima@un.org")
        md_lines.append("- **Principal Engineer**: Stephen Sciortino - stephen.sciortino@un.org")
        md_lines.append("- **GitHub Repository**: [https://github.com/UN-OSAA/osaa-mvp.git](https://github.com/UN-OSAA/osaa-mvp.git)")
        md_lines.append("")
        md_lines.append("**Support Channels**:")
        md_lines.append("")
        md_lines.append("- **Issue Tracking**: GitHub Issues")
        md_lines.append("- **Documentation**: This catalog and repository README")
        md_lines.append("- **Emergency Contact**: UN-OSAA Data Team on-call rotation")
        md_lines.append("")

        return "\n".join(md_lines)

    def generate_json_catalog(self) -> str:
        """Generate JSON format catalog."""
        logger.info("Generating JSON catalog...")

        catalog_json = {
            "generated_at": datetime.now().isoformat(),
            "version": "1.0.0",
            "datasets": []
        }

        for table_name, metadata in self.catalog_data.items():
            dataset = {
                "name": table_name,
                "schema": metadata.schema,
                "table": metadata.name,
                "description": metadata.description,
                "columns": [
                    {
                        "name": col.name,
                        "type": col.type,
                        "nullable": col.nullable,
                        "description": col.description,
                        "business_meaning": col.business_meaning,
                        "example_values": col.example_values
                    }
                    for col in metadata.columns
                ],
                "lineage": {
                    "upstream": metadata.upstream_dependencies,
                    "downstream": metadata.downstream_dependencies
                },
                "metadata": {
                    "update_frequency": metadata.update_frequency,
                    "sla_hours": metadata.sla_hours,
                    "owner": metadata.owner,
                    "grain": metadata.grain
                },
                "physical_properties": metadata.physical_properties,
                "sample_queries": metadata.sample_queries
            }
            catalog_json["datasets"].append(dataset)

        return json.dumps(catalog_json, indent=2)

    def generate_html_catalog(self) -> str:
        """Generate interactive HTML catalog."""
        logger.info("Generating HTML catalog...")

        html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OSAA Data Catalog</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }

        header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px 0;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }

        h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }

        .subtitle {
            font-size: 1.1em;
            opacity: 0.9;
        }

        .search-container {
            margin-bottom: 30px;
            display: flex;
            gap: 10px;
        }

        .search-input {
            flex: 1;
            padding: 12px 20px;
            font-size: 16px;
            border: 2px solid #ddd;
            border-radius: 8px;
            transition: border-color 0.3s;
        }

        .search-input:focus {
            outline: none;
            border-color: #667eea;
        }

        .filter-btn {
            padding: 12px 24px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            transition: background 0.3s;
        }

        .filter-btn:hover {
            background: #5a67d8;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }

        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            transition: transform 0.3s, box-shadow 0.3s;
        }

        .stat-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }

        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }

        .stat-label {
            color: #666;
            margin-top: 5px;
        }

        .dataset-card {
            background: white;
            border-radius: 12px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            overflow: hidden;
            transition: box-shadow 0.3s;
        }

        .dataset-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }

        .dataset-header {
            background: #f8f9fa;
            padding: 20px;
            border-bottom: 2px solid #e9ecef;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .dataset-title {
            font-size: 1.3em;
            font-weight: 600;
            color: #2c3e50;
        }

        .expand-icon {
            font-size: 1.2em;
            transition: transform 0.3s;
        }

        .dataset-header.expanded .expand-icon {
            transform: rotate(180deg);
        }

        .dataset-content {
            padding: 20px;
            display: none;
        }

        .dataset-content.expanded {
            display: block;
        }

        .info-section {
            margin-bottom: 25px;
        }

        .info-section h3 {
            color: #667eea;
            margin-bottom: 10px;
            font-size: 1.1em;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }

        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e9ecef;
        }

        th {
            background: #f8f9fa;
            font-weight: 600;
            color: #495057;
        }

        tr:hover {
            background: #f8f9fa;
        }

        .tag {
            display: inline-block;
            padding: 4px 12px;
            background: #e7f3ff;
            color: #0066cc;
            border-radius: 20px;
            font-size: 0.85em;
            margin: 2px;
        }

        .code-block {
            background: #f6f8fa;
            padding: 15px;
            border-radius: 8px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            overflow-x: auto;
            margin: 10px 0;
        }

        .lineage-graph {
            background: white;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }

        .footer {
            text-align: center;
            padding: 30px;
            color: #666;
            border-top: 2px solid #e9ecef;
            margin-top: 50px;
        }

        @media (max-width: 768px) {
            .container {
                padding: 10px;
            }

            h1 {
                font-size: 1.8em;
            }

            .stats-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <h1>OSAA Data Catalog</h1>
            <div class="subtitle">Comprehensive documentation for the UN-OSAA Data Pipeline</div>
            <div class="subtitle">Generated: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</div>
        </div>
    </header>

    <div class="container">
        <div class="search-container">
            <input type="text" class="search-input" id="searchInput" placeholder="Search datasets, columns, or descriptions...">
            <button class="filter-btn" onclick="searchCatalog()">Search</button>
            <button class="filter-btn" onclick="clearSearch()">Clear</button>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">""" + str(len(self.catalog_data)) + """</div>
                <div class="stat-label">Total Datasets</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">""" + str(sum(len(m.columns) for m in self.catalog_data.values())) + """</div>
                <div class="stat-label">Total Columns</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">4</div>
                <div class="stat-label">Data Sources</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">Daily</div>
                <div class="stat-label">Update Frequency</div>
            </div>
        </div>

        <div class="lineage-graph">
            <h2>Data Pipeline Flow</h2>
            <div class="code-block">
                Raw Data (CSV) → Ingestion → Landing (Parquet) → Transformation → Master Tables → Output (S3)
            </div>
        </div>

        <div id="datasets">
"""

        # Add dataset cards
        for table_name in sorted(self.catalog_data.keys()):
            metadata = self.catalog_data[table_name]
            html_content += f"""
            <div class="dataset-card" data-name="{table_name.lower()}">
                <div class="dataset-header" onclick="toggleDataset(this)">
                    <div>
                        <div class="dataset-title">{table_name}</div>
                        <div style="color: #666; margin-top: 5px;">{metadata.description}</div>
                    </div>
                    <span class="expand-icon">▼</span>
                </div>
                <div class="dataset-content">
                    <div class="info-section">
                        <h3>Dataset Properties</h3>
                        <table>
                            <tr><td><strong>Schema</strong></td><td>{metadata.schema}</td></tr>
                            <tr><td><strong>Table</strong></td><td>{metadata.name}</td></tr>
                            <tr><td><strong>Update Frequency</strong></td><td>{metadata.update_frequency}</td></tr>
                            <tr><td><strong>Owner</strong></td><td>{metadata.owner}</td></tr>
                            <tr><td><strong>SLA</strong></td><td>{metadata.sla_hours} hours</td></tr>
                        </table>
                    </div>

                    <div class="info-section">
                        <h3>Columns</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>Name</th>
                                    <th>Type</th>
                                    <th>Nullable</th>
                                    <th>Description</th>
                                </tr>
                            </thead>
                            <tbody>
"""

            for col in metadata.columns:
                nullable = "Yes" if col.nullable else "No"
                html_content += f"""
                                <tr>
                                    <td><code>{col.name}</code></td>
                                    <td>{col.type}</td>
                                    <td>{nullable}</td>
                                    <td>{col.description}</td>
                                </tr>
"""

            html_content += """
                            </tbody>
                        </table>
                    </div>
"""

            if metadata.upstream_dependencies:
                html_content += """
                    <div class="info-section">
                        <h3>Dependencies</h3>
                        <div><strong>Upstream:</strong></div>
                        <div style="margin-top: 10px;">
"""
                for dep in metadata.upstream_dependencies:
                    html_content += f'<span class="tag">{dep}</span>'
                html_content += """
                        </div>
                    </div>
"""

            if metadata.sample_queries:
                html_content += """
                    <div class="info-section">
                        <h3>Sample Queries</h3>
"""
                for query in metadata.sample_queries[:2]:
                    html_content += f'<div class="code-block">{query}</div>'
                html_content += """
                    </div>
"""

            html_content += """
                </div>
            </div>
"""

        html_content += """
        </div>
    </div>

    <div class="footer">
        <p>© 2025 United Nations Office of the Special Adviser on Africa (UN-OSAA)</p>
        <p>For questions or support, contact the <a href="mailto:stephen.sciortino@un.org">Data Team</a></p>
    </div>

    <script>
        function toggleDataset(header) {
            header.classList.toggle('expanded');
            const content = header.nextElementSibling;
            content.classList.toggle('expanded');
        }

        function searchCatalog() {
            const searchTerm = document.getElementById('searchInput').value.toLowerCase();
            const datasets = document.querySelectorAll('.dataset-card');

            datasets.forEach(dataset => {
                const text = dataset.textContent.toLowerCase();
                const name = dataset.getAttribute('data-name');

                if (text.includes(searchTerm) || name.includes(searchTerm)) {
                    dataset.style.display = 'block';
                } else {
                    dataset.style.display = 'none';
                }
            });
        }

        function clearSearch() {
            document.getElementById('searchInput').value = '';
            const datasets = document.querySelectorAll('.dataset-card');
            datasets.forEach(dataset => {
                dataset.style.display = 'block';
            });
        }

        // Enable search on Enter key
        document.getElementById('searchInput').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchCatalog();
            }
        });
    </script>
</body>
</html>
"""

        return html_content

    def save_catalogs(self) -> None:
        """Save all catalog formats to files."""
        # Create docs directory if it doesn't exist
        self.docs_dir.mkdir(parents=True, exist_ok=True)

        # Save Markdown catalog
        md_catalog = self.generate_markdown_catalog()
        md_path = self.docs_dir / "DATA_CATALOG.md"
        with open(md_path, 'w') as f:
            f.write(md_catalog)
        logger.info(f"Saved Markdown catalog to {md_path}")

        # Save JSON catalog
        json_catalog = self.generate_json_catalog()
        json_path = self.docs_dir / "catalog.json"
        with open(json_path, 'w') as f:
            f.write(json_catalog)
        logger.info(f"Saved JSON catalog to {json_path}")

        # Save HTML catalog
        html_catalog = self.generate_html_catalog()
        html_path = self.docs_dir / "catalog.html"
        with open(html_path, 'w') as f:
            f.write(html_catalog)
        logger.info(f"Saved HTML catalog to {html_path}")

    def run(self) -> None:
        """Main execution method."""
        logger.info("Starting Data Catalog Generation...")

        try:
            # Scan models
            self.scan_models()

            # Build lineage
            self.build_lineage()

            # Save catalogs
            self.save_catalogs()

            logger.info("Data Catalog Generation Complete!")
            logger.info(f"Total datasets cataloged: {len(self.catalog_data)}")

        except Exception as e:
            logger.error(f"Error generating catalog: {e}")
            raise


def main():
    """Main entry point for the script."""
    # Get project root (assuming script is in scripts/ directory)
    project_root = Path(__file__).parent.parent

    # Create and run generator
    generator = DataCatalogGenerator(str(project_root))
    generator.run()


if __name__ == "__main__":
    main()