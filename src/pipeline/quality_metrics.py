"""Data quality metrics calculation and tracking module.

This module provides comprehensive data quality metrics for the UN-OSAA
indicator datasets including completeness, null rates, duplicate detection,
and trend analysis.
"""

import duckdb
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json
from dataclasses import dataclass, asdict

from pipeline.logging_config import create_logger

logger = create_logger(__name__)


@dataclass
class DatasetMetrics:
    """Data quality metrics for a single dataset."""

    dataset_name: str
    timestamp: str
    total_records: int
    total_indicators: int
    total_countries: int
    total_years: int
    completeness_percentage: float
    null_rate_percentage: float
    duplicate_count: int
    year_range_min: int
    year_range_max: int
    quality_score: float
    issues: List[str]


class QualityMetrics:
    """Calculate and track data quality metrics for indicator datasets."""

    def __init__(self, connection: Optional[duckdb.DuckDBPyConnection] = None):
        """Initialize quality metrics calculator.

        Args:
            connection: DuckDB connection. If None, creates a new connection.
        """
        self.con = connection if connection else duckdb.connect()
        logger.info("Quality metrics calculator initialized")

    def calculate_completeness_percentage(self, table_name: str,
                                         value_column: str = 'value') -> float:
        """Calculate completeness percentage for a dataset.

        Args:
            table_name: Fully qualified table name (e.g., 'sdg.data_national')
            value_column: Name of the value column to check for nulls

        Returns:
            Completeness percentage (0-100)
        """
        try:
            query = f"""
                SELECT
                    100.0 * SUM(CASE WHEN {value_column} IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) as completeness
                FROM {table_name}
            """
            result = self.con.execute(query).fetchone()
            completeness = float(result[0]) if result and result[0] is not None else 0.0
            logger.info(f"Completeness for {table_name}: {completeness:.2f}%")
            return completeness
        except Exception as e:
            logger.error(f"Error calculating completeness for {table_name}: {e}")
            return 0.0

    def calculate_null_rate(self, table_name: str, columns: List[str]) -> Dict[str, float]:
        """Calculate null rate for specified columns.

        Args:
            table_name: Fully qualified table name
            columns: List of column names to check

        Returns:
            Dictionary mapping column names to null rate percentages
        """
        null_rates = {}
        try:
            for column in columns:
                query = f"""
                    SELECT
                        100.0 * SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) / COUNT(*) as null_rate
                    FROM {table_name}
                """
                result = self.con.execute(query).fetchone()
                null_rates[column] = float(result[0]) if result and result[0] is not None else 0.0

            logger.info(f"Null rates for {table_name}: {null_rates}")
            return null_rates
        except Exception as e:
            logger.error(f"Error calculating null rates for {table_name}: {e}")
            return {col: 0.0 for col in columns}

    def count_duplicates(self, table_name: str, grain_columns: List[str]) -> int:
        """Count duplicate records based on grain columns.

        Args:
            table_name: Fully qualified table name
            grain_columns: List of columns that define the grain

        Returns:
            Number of duplicate records
        """
        try:
            grain_str = ', '.join(grain_columns)
            query = f"""
                SELECT COUNT(*) as duplicate_count
                FROM (
                    SELECT {grain_str}, COUNT(*) as cnt
                    FROM {table_name}
                    GROUP BY {grain_str}
                    HAVING COUNT(*) > 1
                )
            """
            result = self.con.execute(query).fetchone()
            duplicates = int(result[0]) if result and result[0] is not None else 0
            logger.info(f"Duplicates in {table_name}: {duplicates}")
            return duplicates
        except Exception as e:
            logger.error(f"Error counting duplicates for {table_name}: {e}")
            return 0

    def get_year_range(self, table_name: str, year_column: str = 'year') -> Tuple[int, int]:
        """Get the year range for a dataset.

        Args:
            table_name: Fully qualified table name
            year_column: Name of the year column

        Returns:
            Tuple of (min_year, max_year)
        """
        try:
            query = f"""
                SELECT MIN({year_column}), MAX({year_column})
                FROM {table_name}
            """
            result = self.con.execute(query).fetchone()
            if result and result[0] is not None and result[1] is not None:
                return (int(result[0]), int(result[1]))
            return (0, 0)
        except Exception as e:
            logger.error(f"Error getting year range for {table_name}: {e}")
            return (0, 0)

    def calculate_quality_score(self, completeness: float, null_rate: float,
                               duplicate_count: int, total_records: int) -> float:
        """Calculate overall quality score based on multiple metrics.

        Quality score is calculated as:
        - 50% weight: Completeness percentage
        - 30% weight: Inverted null rate (100 - null_rate)
        - 20% weight: Duplicate penalty (penalize if > 0.1% duplicates)

        Args:
            completeness: Completeness percentage (0-100)
            null_rate: Null rate percentage (0-100)
            duplicate_count: Number of duplicate records
            total_records: Total number of records

        Returns:
            Quality score (0-100)
        """
        # Completeness contributes 50%
        completeness_score = completeness * 0.5

        # Low null rate contributes 30%
        null_score = (100 - null_rate) * 0.3

        # Duplicate penalty contributes 20%
        duplicate_rate = (duplicate_count / total_records * 100) if total_records > 0 else 0
        duplicate_score = max(0, (100 - duplicate_rate * 10)) * 0.2

        quality_score = completeness_score + null_score + duplicate_score
        return round(quality_score, 2)

    def calculate_dataset_metrics(self, table_name: str,
                                  dataset_type: str = 'data_national') -> DatasetMetrics:
        """Calculate comprehensive metrics for a dataset.

        Args:
            table_name: Fully qualified table name (e.g., 'sdg.data_national')
            dataset_type: Type of dataset ('data_national' or 'wdi')

        Returns:
            DatasetMetrics object with all calculated metrics
        """
        logger.info(f"Calculating metrics for {table_name}")
        issues = []

        try:
            # Get basic counts
            if dataset_type == 'data_national':
                count_query = """
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(DISTINCT indicator_id) as total_indicators,
                        COUNT(DISTINCT country_id) as total_countries,
                        COUNT(DISTINCT year) as total_years
                    FROM {}
                """.format(table_name)
                grain_columns = ['indicator_id', 'country_id', 'year']
                value_column = 'value'
                year_column = 'year'
            else:  # wdi
                count_query = """
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(DISTINCT "Indicator Code") as total_indicators,
                        COUNT(DISTINCT "Country Code") as total_countries,
                        0 as total_years
                    FROM {}
                """.format(table_name)
                grain_columns = ['"Indicator Code"', '"Country Code"']
                value_column = '"1960"'  # Use first year column as proxy
                year_column = None

            result = self.con.execute(count_query).fetchone()
            total_records = int(result[0]) if result else 0
            total_indicators = int(result[1]) if result else 0
            total_countries = int(result[2]) if result else 0
            total_years = int(result[3]) if result else 0

            # Calculate quality metrics
            completeness = self.calculate_completeness_percentage(table_name, value_column)
            null_rates = self.calculate_null_rate(table_name, [value_column])
            avg_null_rate = sum(null_rates.values()) / len(null_rates) if null_rates else 0
            duplicate_count = self.count_duplicates(table_name, grain_columns)

            # Get year range for data_national datasets
            if year_column:
                year_min, year_max = self.get_year_range(table_name, year_column)
            else:
                year_min, year_max = 1960, 2023  # WDI default range

            # Calculate quality score
            quality_score = self.calculate_quality_score(
                completeness, avg_null_rate, duplicate_count, total_records
            )

            # Identify issues
            if completeness < 80:
                issues.append(f"Low completeness: {completeness:.2f}%")
            if avg_null_rate > 20:
                issues.append(f"High null rate: {avg_null_rate:.2f}%")
            if duplicate_count > 0:
                issues.append(f"Found {duplicate_count} duplicate records")
            if year_column and (year_min < 1960 or year_max > 2030):
                issues.append(f"Year range outside expected bounds: {year_min}-{year_max}")

            metrics = DatasetMetrics(
                dataset_name=table_name,
                timestamp=datetime.now().isoformat(),
                total_records=total_records,
                total_indicators=total_indicators,
                total_countries=total_countries,
                total_years=total_years,
                completeness_percentage=round(completeness, 2),
                null_rate_percentage=round(avg_null_rate, 2),
                duplicate_count=duplicate_count,
                year_range_min=year_min,
                year_range_max=year_max,
                quality_score=quality_score,
                issues=issues
            )

            logger.info(f"Metrics calculated for {table_name}: Quality Score = {quality_score}")
            return metrics

        except Exception as e:
            logger.error(f"Error calculating metrics for {table_name}: {e}")
            return DatasetMetrics(
                dataset_name=table_name,
                timestamp=datetime.now().isoformat(),
                total_records=0,
                total_indicators=0,
                total_countries=0,
                total_years=0,
                completeness_percentage=0.0,
                null_rate_percentage=0.0,
                duplicate_count=0,
                year_range_min=0,
                year_range_max=0,
                quality_score=0.0,
                issues=[f"Error calculating metrics: {str(e)}"]
            )

    def calculate_all_metrics(self) -> Dict[str, DatasetMetrics]:
        """Calculate metrics for all indicator datasets.

        Returns:
            Dictionary mapping dataset names to their metrics
        """
        logger.info("Calculating metrics for all datasets")

        all_metrics = {}

        # SDG and OPRI datasets
        for dataset in ['sdg.data_national', 'opri.data_national']:
            try:
                metrics = self.calculate_dataset_metrics(dataset, 'data_national')
                all_metrics[dataset] = metrics
            except Exception as e:
                logger.error(f"Failed to calculate metrics for {dataset}: {e}")

        # WDI dataset
        try:
            metrics = self.calculate_dataset_metrics('wdi.csv', 'wdi')
            all_metrics['wdi.csv'] = metrics
        except Exception as e:
            logger.error(f"Failed to calculate metrics for wdi.csv: {e}")

        return all_metrics

    def export_metrics_json(self, metrics: Dict[str, DatasetMetrics],
                           output_path: str) -> None:
        """Export metrics to JSON file.

        Args:
            metrics: Dictionary of dataset metrics
            output_path: Path to output JSON file
        """
        try:
            metrics_dict = {name: asdict(metric) for name, metric in metrics.items()}
            with open(output_path, 'w') as f:
                json.dump(metrics_dict, f, indent=2)
            logger.info(f"Metrics exported to {output_path}")
        except Exception as e:
            logger.error(f"Error exporting metrics: {e}")


if __name__ == "__main__":
    # Example usage
    qm = QualityMetrics()
    all_metrics = qm.calculate_all_metrics()

    print("\n=== Data Quality Metrics Summary ===\n")
    for dataset_name, metrics in all_metrics.items():
        print(f"\nDataset: {dataset_name}")
        print(f"  Quality Score: {metrics.quality_score}/100")
        print(f"  Total Records: {metrics.total_records:,}")
        print(f"  Indicators: {metrics.total_indicators}")
        print(f"  Countries: {metrics.total_countries}")
        print(f"  Completeness: {metrics.completeness_percentage}%")
        print(f"  Null Rate: {metrics.null_rate_percentage}%")
        print(f"  Duplicates: {metrics.duplicate_count}")
        if metrics.issues:
            print(f"  Issues: {', '.join(metrics.issues)}")
