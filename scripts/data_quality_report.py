#!/usr/bin/env python3
"""Data quality report generation script.

This script generates comprehensive data quality reports for UN-OSAA indicator
datasets including metrics calculation, trend analysis, and HTML report generation.

Usage:
    python scripts/data_quality_report.py --format html --output reports/quality_report.html
    python scripts/data_quality_report.py --format json --output reports/quality_metrics.json
    python scripts/data_quality_report.py --format console
"""

import argparse
import sys
import os
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

import duckdb
from pipeline.quality_metrics import QualityMetrics, DatasetMetrics
from pipeline.logging_config import create_logger

logger = create_logger(__name__)


class DataQualityReporter:
    """Generate comprehensive data quality reports."""

    def __init__(self, connection: duckdb.DuckDBPyConnection = None):
        """Initialize the data quality reporter.

        Args:
            connection: DuckDB connection. If None, creates a new connection.
        """
        self.con = connection if connection else duckdb.connect()
        self.metrics_calculator = QualityMetrics(self.con)
        logger.info("Data quality reporter initialized")

    def generate_html_report(self, metrics: dict, output_path: str) -> None:
        """Generate HTML report with data quality metrics.

        Args:
            metrics: Dictionary of DatasetMetrics objects
            output_path: Path to output HTML file
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Calculate aggregate statistics
        total_quality_score = sum(m.quality_score for m in metrics.values()) / len(metrics)
        total_records = sum(m.total_records for m in metrics.values())
        total_issues = sum(len(m.issues) for m in metrics.values())

        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UN-OSAA Data Quality Report</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 0;
        }}
        .dataset-card {{
            background: #ffffff;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
        }}
        .dataset-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}
        .dataset-name {{
            font-size: 20px;
            font-weight: bold;
            color: #2c3e50;
        }}
        .quality-score {{
            font-size: 28px;
            font-weight: bold;
            padding: 10px 20px;
            border-radius: 8px;
        }}
        .score-excellent {{ background: #2ecc71; color: white; }}
        .score-good {{ background: #f39c12; color: white; }}
        .score-poor {{ background: #e74c3c; color: white; }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 15px 0;
        }}
        .metric {{
            padding: 15px;
            background: #f8f9fa;
            border-radius: 4px;
            border-left: 4px solid #3498db;
        }}
        .metric-label {{
            font-size: 12px;
            color: #7f8c8d;
            margin-bottom: 5px;
        }}
        .metric-value {{
            font-size: 20px;
            font-weight: bold;
            color: #2c3e50;
        }}
        .issues {{
            margin-top: 15px;
            padding: 15px;
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            border-radius: 4px;
        }}
        .issues ul {{
            margin: 10px 0;
            padding-left: 20px;
        }}
        .issues li {{
            color: #856404;
            margin: 5px 0;
        }}
        .no-issues {{
            background: #d4edda;
            border-left-color: #28a745;
        }}
        .no-issues p {{
            color: #155724;
            margin: 0;
        }}
        .timestamp {{
            text-align: right;
            color: #7f8c8d;
            font-size: 14px;
            margin-top: 30px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #3498db;
            color: white;
            font-weight: bold;
        }}
        tr:hover {{
            background: #f5f5f5;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>UN-OSAA Data Quality Report</h1>

        <div class="summary">
            <div class="summary-card">
                <h3>OVERALL QUALITY SCORE</h3>
                <p class="value">{total_quality_score:.1f}/100</p>
            </div>
            <div class="summary-card" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
                <h3>TOTAL RECORDS</h3>
                <p class="value">{total_records:,}</p>
            </div>
            <div class="summary-card" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
                <h3>DATASETS MONITORED</h3>
                <p class="value">{len(metrics)}</p>
            </div>
            <div class="summary-card" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);">
                <h3>ISSUES DETECTED</h3>
                <p class="value">{total_issues}</p>
            </div>
        </div>

        <h2>Dataset Details</h2>
"""

        # Add dataset cards
        for dataset_name, dataset_metrics in metrics.items():
            score_class = 'score-excellent' if dataset_metrics.quality_score >= 80 else \
                         'score-good' if dataset_metrics.quality_score >= 60 else 'score-poor'

            html += f"""
        <div class="dataset-card">
            <div class="dataset-header">
                <div class="dataset-name">{dataset_name}</div>
                <div class="quality-score {score_class}">{dataset_metrics.quality_score}/100</div>
            </div>

            <div class="metrics-grid">
                <div class="metric">
                    <div class="metric-label">Total Records</div>
                    <div class="metric-value">{dataset_metrics.total_records:,}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Indicators</div>
                    <div class="metric-value">{dataset_metrics.total_indicators}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Countries</div>
                    <div class="metric-value">{dataset_metrics.total_countries}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Years</div>
                    <div class="metric-value">{dataset_metrics.total_years if dataset_metrics.total_years > 0 else 'N/A'}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Completeness</div>
                    <div class="metric-value">{dataset_metrics.completeness_percentage}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Null Rate</div>
                    <div class="metric-value">{dataset_metrics.null_rate_percentage}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Duplicates</div>
                    <div class="metric-value">{dataset_metrics.duplicate_count}</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Year Range</div>
                    <div class="metric-value">{dataset_metrics.year_range_min}-{dataset_metrics.year_range_max}</div>
                </div>
            </div>
"""

            # Add issues section
            if dataset_metrics.issues:
                html += """
            <div class="issues">
                <strong>Issues Detected:</strong>
                <ul>
"""
                for issue in dataset_metrics.issues:
                    html += f"                    <li>{issue}</li>\n"
                html += """
                </ul>
            </div>
"""
            else:
                html += """
            <div class="issues no-issues">
                <p>No issues detected</p>
            </div>
"""

            html += """
        </div>
"""

        # Close HTML
        html += f"""
        <div class="timestamp">
            Report generated: {timestamp}
        </div>
    </div>
</body>
</html>
"""

        # Write to file
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(html)
            logger.info(f"HTML report generated: {output_path}")
            print(f"\nHTML report generated: {output_path}")
        except Exception as e:
            logger.error(f"Error writing HTML report: {e}")
            raise

    def generate_console_report(self, metrics: dict) -> None:
        """Print data quality report to console.

        Args:
            metrics: Dictionary of DatasetMetrics objects
        """
        print("\n" + "=" * 80)
        print("UN-OSAA DATA QUALITY REPORT")
        print("=" * 80)
        print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Overall summary
        total_quality_score = sum(m.quality_score for m in metrics.values()) / len(metrics)
        total_records = sum(m.total_records for m in metrics.values())
        total_issues = sum(len(m.issues) for m in metrics.values())

        print(f"Overall Quality Score: {total_quality_score:.1f}/100")
        print(f"Total Records: {total_records:,}")
        print(f"Datasets Monitored: {len(metrics)}")
        print(f"Issues Detected: {total_issues}\n")

        # Dataset details
        for dataset_name, dataset_metrics in metrics.items():
            print("-" * 80)
            print(f"\nDataset: {dataset_name}")
            print(f"Quality Score: {dataset_metrics.quality_score}/100")
            print(f"\nMetrics:")
            print(f"  Total Records: {dataset_metrics.total_records:,}")
            print(f"  Indicators: {dataset_metrics.total_indicators}")
            print(f"  Countries: {dataset_metrics.total_countries}")
            print(f"  Years: {dataset_metrics.total_years if dataset_metrics.total_years > 0 else 'N/A'}")
            print(f"  Completeness: {dataset_metrics.completeness_percentage}%")
            print(f"  Null Rate: {dataset_metrics.null_rate_percentage}%")
            print(f"  Duplicates: {dataset_metrics.duplicate_count}")
            print(f"  Year Range: {dataset_metrics.year_range_min}-{dataset_metrics.year_range_max}")

            if dataset_metrics.issues:
                print(f"\nIssues:")
                for issue in dataset_metrics.issues:
                    print(f"  - {issue}")
            else:
                print(f"\nNo issues detected")
            print()

        print("=" * 80 + "\n")

    def run_report(self, format: str = 'console', output_path: str = None) -> None:
        """Run data quality report generation.

        Args:
            format: Output format ('html', 'json', or 'console')
            output_path: Path to output file (required for html and json formats)
        """
        logger.info(f"Generating data quality report in {format} format")

        # Calculate metrics for all datasets
        metrics = self.metrics_calculator.calculate_all_metrics()

        if format == 'html':
            if not output_path:
                output_path = 'reports/quality_report.html'
            self.generate_html_report(metrics, output_path)

        elif format == 'json':
            if not output_path:
                output_path = 'reports/quality_metrics.json'
            self.metrics_calculator.export_metrics_json(metrics, output_path)
            print(f"\nJSON metrics exported: {output_path}")

        elif format == 'console':
            self.generate_console_report(metrics)

        else:
            raise ValueError(f"Unknown format: {format}")


def main():
    """Main entry point for data quality report script."""
    parser = argparse.ArgumentParser(
        description='Generate data quality reports for UN-OSAA indicator datasets'
    )
    parser.add_argument(
        '--format',
        choices=['html', 'json', 'console'],
        default='console',
        help='Output format (default: console)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path (required for html and json formats)'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.format in ['html', 'json'] and not args.output:
        parser.error(f"--output is required for {args.format} format")

    try:
        reporter = DataQualityReporter()
        reporter.run_report(format=args.format, output_path=args.output)
        print("\nData quality report completed successfully!")

    except Exception as e:
        logger.error(f"Error generating data quality report: {e}")
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
