#!/usr/bin/env python3
"""Utility script to check data freshness for OSAA pipeline.

This script checks the freshness of all datasets and optionally sends alerts
for stale data.
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.freshness_monitor import FreshnessMonitor
from pipeline.alerting import get_alert_manager
from pipeline.config import DB_PATH, TARGET
from pipeline.logging_config import create_logger

logger = create_logger(__name__)


def check_freshness(
    db_path: str = None,
    send_alerts: bool = False,
    verbose: bool = False
) -> bool:
    """Check data freshness and optionally send alerts.

    Args:
        db_path: Path to DuckDB database
        send_alerts: Whether to send alerts for stale data
        verbose: Print detailed output

    Returns:
        True if all data is fresh, False otherwise
    """
    # Initialize monitor
    monitor = FreshnessMonitor(db_path=db_path or DB_PATH)
    alert_manager = get_alert_manager() if send_alerts else None

    try:
        # Check all datasets
        results = monitor.check_all_datasets()

        # Print report
        if verbose:
            print(monitor.generate_freshness_report())
        else:
            # Compact summary
            fresh_count = sum(1 for r in results.values() if r.get('is_fresh', False))
            total_count = len(results)

            print(f"\nData Freshness Check ({TARGET} environment)")
            print(f"Fresh: {fresh_count}/{total_count} datasets")

            # Show stale datasets
            stale = [name for name, info in results.items() if not info.get('is_fresh', False)]
            if stale:
                print(f"\nStale datasets:")
                for dataset in stale:
                    info = results[dataset]
                    hours = info.get('hours_old', 0)
                    sla = info.get('sla_hours', 24)
                    print(f"  - {dataset}: {hours:.1f}h old (SLA: {sla}h)")

        # Send alerts for stale data
        if send_alerts and alert_manager:
            for dataset, info in results.items():
                if not info.get('is_fresh', False) and info.get('last_update'):
                    logger.info(f"Sending alert for stale dataset: {dataset}")
                    alert_manager.send_freshness_alert(
                        dataset=dataset,
                        hours_old=info['hours_old'],
                        sla_hours=info['sla_hours'],
                        last_update=info['last_update']
                    )

        # Return overall status
        all_fresh = all(r.get('is_fresh', False) for r in results.values())
        return all_fresh

    finally:
        monitor.disconnect()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check data freshness for OSAA pipeline"
    )
    parser.add_argument(
        "--db-path",
        help="Path to DuckDB database (default: from config)"
    )
    parser.add_argument(
        "--send-alerts",
        action="store_true",
        help="Send alerts for stale data"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print detailed report"
    )
    parser.add_argument(
        "--fail-on-stale",
        action="store_true",
        help="Exit with error code if data is stale"
    )

    args = parser.parse_args()

    try:
        all_fresh = check_freshness(
            db_path=args.db_path,
            send_alerts=args.send_alerts,
            verbose=args.verbose
        )

        if args.fail_on_stale and not all_fresh:
            print("\nERROR: Stale data detected")
            sys.exit(1)

        if all_fresh:
            print("\nAll datasets are fresh!")
            sys.exit(0)
        else:
            print("\nWARNING: Some datasets are stale")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Freshness check failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
