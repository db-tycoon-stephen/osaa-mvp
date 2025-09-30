#!/usr/bin/env python3
"""
Backfill timestamps for existing data in the pipeline.

This script adds loaded_at and file_modified_at timestamps to existing records
that were loaded before the incremental processing implementation.

Usage:
    python scripts/backfill_timestamps.py [--dry-run] [--batch-size 10000]

Options:
    --dry-run       Show what would be updated without making changes
    --batch-size    Number of records to process per batch (default: 10000)
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb


def get_connection(db_path: str = "sqlMesh/unosaa_data_pipeline.db") -> duckdb.DuckDBPyConnection:
    """Create a connection to the DuckDB database."""
    return duckdb.connect(db_path, read_only=False)


def backfill_table(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    dry_run: bool = False,
    batch_size: int = 10000
) -> int:
    """
    Backfill timestamps for a specific table.

    Args:
        conn: DuckDB connection
        schema: Schema name
        table: Table name
        dry_run: If True, only report what would be done
        batch_size: Number of records to update per batch

    Returns:
        Number of records updated
    """
    full_table_name = f"{schema}.{table}"

    # Check if table exists
    try:
        count_query = f"SELECT COUNT(*) FROM {full_table_name}"
        result = conn.execute(count_query).fetchone()
        total_records = result[0] if result else 0

        if total_records == 0:
            print(f"  ⚠ {full_table_name} is empty, skipping")
            return 0

    except Exception as e:
        print(f"  ⚠ {full_table_name} does not exist or is not accessible: {e}")
        return 0

    # Check if timestamp columns exist
    try:
        columns_query = f"PRAGMA table_info('{full_table_name}')"
        columns = conn.execute(columns_query).fetchall()
        column_names = [col[1] for col in columns]

        has_loaded_at = "loaded_at" in column_names
        has_file_modified_at = "file_modified_at" in column_names

        if not has_loaded_at and not has_file_modified_at:
            print(f"  ⚠ {full_table_name} does not have timestamp columns, skipping")
            return 0

    except Exception as e:
        print(f"  ⚠ Could not check columns for {full_table_name}: {e}")
        return 0

    # Count records that need backfilling
    null_check = []
    if has_loaded_at:
        null_check.append("loaded_at IS NULL")
    if has_file_modified_at:
        null_check.append("file_modified_at IS NULL")

    if not null_check:
        print(f"  ⚠ {full_table_name} has no timestamp columns to backfill")
        return 0

    null_condition = " OR ".join(null_check)
    count_null_query = f"SELECT COUNT(*) FROM {full_table_name} WHERE {null_condition}"

    try:
        result = conn.execute(count_null_query).fetchone()
        records_to_update = result[0] if result else 0

        if records_to_update == 0:
            print(f"  ✓ {full_table_name} - all records already have timestamps")
            return 0

        print(f"  → {full_table_name} - found {records_to_update:,} records to backfill")

        if dry_run:
            print(f"    [DRY RUN] Would update {records_to_update:,} records")
            return records_to_update

        # Perform the backfill
        # Use a historical timestamp to indicate these are backfilled records
        backfill_timestamp = datetime(2024, 1, 1, 0, 0, 0)

        update_clauses = []
        if has_loaded_at:
            update_clauses.append(f"loaded_at = '{backfill_timestamp}'")
        if has_file_modified_at:
            update_clauses.append(f"file_modified_at = '{backfill_timestamp}'")

        update_statement = ", ".join(update_clauses)
        update_query = f"""
            UPDATE {full_table_name}
            SET {update_statement}
            WHERE {null_condition}
        """

        conn.execute(update_query)
        print(f"    ✓ Updated {records_to_update:,} records")

        return records_to_update

    except Exception as e:
        print(f"  ✗ Error backfilling {full_table_name}: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Backfill timestamps for existing data in the pipeline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without making changes"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of records to process per batch (default: 10000)"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="sqlMesh/unosaa_data_pipeline.db",
        help="Path to DuckDB database file"
    )

    args = parser.parse_args()

    # Tables to backfill (schema, table_name)
    tables_to_backfill = [
        ("sdg", "data_national"),
        ("opri", "data_national"),
        ("wdi", "csv"),
        ("sources", "sdg"),
        ("sources", "opri"),
        ("sources", "wdi"),
        ("master", "indicators"),
    ]

    print("=" * 70)
    print("TIMESTAMP BACKFILL SCRIPT")
    print("=" * 70)
    print(f"Database: {args.db_path}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE UPDATE'}")
    print(f"Batch size: {args.batch_size:,}")
    print()

    try:
        conn = get_connection(args.db_path)
        print("✓ Connected to database")
        print()

        total_updated = 0

        for schema, table in tables_to_backfill:
            updated = backfill_table(
                conn, schema, table,
                dry_run=args.dry_run,
                batch_size=args.batch_size
            )
            total_updated += updated

        print()
        print("=" * 70)
        print(f"SUMMARY: {total_updated:,} total records {'would be ' if args.dry_run else ''}updated")
        print("=" * 70)

        if args.dry_run:
            print("\nTo perform the actual backfill, run without --dry-run")

        conn.close()
        return 0

    except Exception as e:
        print(f"\n✗ Fatal error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())