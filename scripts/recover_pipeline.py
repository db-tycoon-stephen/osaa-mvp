#!/usr/bin/env python3
"""Recovery tool for OSAA data pipeline operations.

This CLI tool provides manual recovery operations for the pipeline,
including:
- List failed operations and checkpoints
- Retry specific files or operations
- Clear checkpoints to force re-processing
- Rollback partial changes
- View checkpoint statistics and history

Usage:
    python scripts/recover_pipeline.py list-failed --pipeline ingest
    python scripts/recover_pipeline.py retry --pipeline ingest --file path/to/file.csv
    python scripts/recover_pipeline.py clear --pipeline ingest --scope file --key s3://bucket/file
    python scripts/recover_pipeline.py stats --pipeline ingest
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.checkpoint import CheckpointScope, CheckpointStatus, PipelineCheckpoint
from pipeline.logging_config import create_logger

logger = create_logger(__name__)


def list_failed_operations(pipeline_name: str, scope: Optional[str] = None) -> None:
    """List all failed operations for a pipeline.

    Args:
        pipeline_name: Name of the pipeline
        scope: Optional scope filter
    """
    logger.info(f"Listing failed operations for pipeline: {pipeline_name}")

    checkpoint = PipelineCheckpoint(pipeline_name=pipeline_name)

    # Get all pending and failed checkpoints
    scope_enum = CheckpointScope(scope) if scope else None
    pending = checkpoint.get_pending(scope=scope_enum)

    if not pending:
        logger.info("No failed or pending operations found")
        return

    logger.info(f"\nFound {len(pending)} failed/pending operations:\n")

    for item in pending:
        print(f"Checkpoint ID: {item['checkpoint_id']}")
        print(f"  Scope: {item['scope']}")
        print(f"  Scope Key: {item['scope_key']}")
        print(f"  File Path: {item['file_path'] or 'N/A'}")
        print(f"  Status: {item['status']}")
        print(f"  Error: {item['error_message'] or 'N/A'}")
        print(f"  Created: {item['created_at']}")
        print(f"  Updated: {item['updated_at']}")
        print()

    checkpoint.close()


def list_completed_operations(
    pipeline_name: str,
    scope: Optional[str] = None,
    limit: int = 20
) -> None:
    """List completed operations for a pipeline.

    Args:
        pipeline_name: Name of the pipeline
        scope: Optional scope filter
        limit: Maximum number of results to show
    """
    logger.info(f"Listing completed operations for pipeline: {pipeline_name}")

    checkpoint = PipelineCheckpoint(pipeline_name=pipeline_name)

    scope_enum = CheckpointScope(scope) if scope else None
    completed = checkpoint.get_completed(scope=scope_enum)

    if not completed:
        logger.info("No completed operations found")
        return

    # Limit results
    completed = completed[:limit]

    logger.info(f"\nShowing {len(completed)} most recent completed operations:\n")

    for item in completed:
        print(f"Checkpoint ID: {item['checkpoint_id']}")
        print(f"  Scope: {item['scope']}")
        print(f"  Scope Key: {item['scope_key']}")
        print(f"  File Path: {item['file_path'] or 'N/A'}")
        print(f"  Checksum: {item['checksum'] or 'N/A'}")
        print(f"  Completed: {item['completed_at']}")
        print()

    checkpoint.close()


def retry_operation(
    pipeline_name: str,
    scope: str,
    scope_key: str,
    file_path: Optional[str] = None
) -> None:
    """Retry a specific operation by clearing its checkpoint.

    Args:
        pipeline_name: Name of the pipeline
        scope: Checkpoint scope
        scope_key: Scope key identifier
        file_path: Optional file path
    """
    logger.info(f"Retrying operation: {scope}/{scope_key}")

    checkpoint = PipelineCheckpoint(pipeline_name=pipeline_name)

    # Get current status
    scope_enum = CheckpointScope(scope)
    status = checkpoint.get_status(scope_enum, scope_key, file_path)

    if status is None:
        logger.warning(f"No checkpoint found for {scope}/{scope_key}")
        checkpoint.close()
        return

    logger.info(f"Current status: {status.value}")

    # Clear the checkpoint
    checkpoint.clear_checkpoint(scope_enum, scope_key, file_path)

    logger.info(f"Checkpoint cleared. Operation can now be retried.")
    logger.info(f"Run the pipeline again to retry this operation.")

    checkpoint.close()


def clear_checkpoints(
    pipeline_name: str,
    scope: Optional[str] = None,
    scope_key: Optional[str] = None,
    status: Optional[str] = None,
    confirm: bool = False
) -> None:
    """Clear checkpoints matching the given filters.

    Args:
        pipeline_name: Name of the pipeline
        scope: Optional scope filter
        scope_key: Optional scope key filter
        status: Optional status filter
        confirm: Whether to skip confirmation prompt
    """
    logger.info(f"Clearing checkpoints for pipeline: {pipeline_name}")

    checkpoint = PipelineCheckpoint(pipeline_name=pipeline_name)

    # Build filter description
    filters = []
    if scope:
        filters.append(f"scope={scope}")
    if scope_key:
        filters.append(f"scope_key={scope_key}")
    if status:
        filters.append(f"status={status}")

    filter_desc = ", ".join(filters) if filters else "ALL"

    # Get count before clearing
    if not confirm:
        # Preview what will be cleared
        if status:
            if status == CheckpointStatus.COMPLETED.value:
                to_clear = checkpoint.get_completed(
                    CheckpointScope(scope) if scope else None,
                    scope_key
                )
            else:
                to_clear = checkpoint.get_pending(
                    CheckpointScope(scope) if scope else None,
                    scope_key
                )
            count = len(to_clear)
        else:
            stats = checkpoint.get_statistics()
            count = sum(s['count'] for s in stats['by_status'].values())

        print(f"\nThis will clear {count} checkpoint(s) with filters: {filter_desc}")
        response = input("Are you sure? (yes/no): ")

        if response.lower() != "yes":
            logger.info("Operation cancelled")
            checkpoint.close()
            return

    # Clear checkpoints
    count = checkpoint.clear_all_checkpoints(
        CheckpointScope(scope) if scope else None,
        scope_key,
        CheckpointStatus(status) if status else None
    )

    logger.info(f"Cleared {count} checkpoint(s)")

    checkpoint.close()


def show_statistics(pipeline_name: str) -> None:
    """Show checkpoint statistics for a pipeline.

    Args:
        pipeline_name: Name of the pipeline
    """
    logger.info(f"Checkpoint statistics for pipeline: {pipeline_name}")

    checkpoint = PipelineCheckpoint(pipeline_name=pipeline_name)

    stats = checkpoint.get_statistics()

    print(f"\nPipeline: {stats['pipeline_name']}\n")
    print("Checkpoint Status Summary:")
    print("-" * 60)

    if not stats['by_status']:
        print("No checkpoints found")
    else:
        for status, data in stats['by_status'].items():
            print(f"\n{status.upper()}:")
            print(f"  Count: {data['count']}")
            print(f"  First Created: {data['first_created']}")
            print(f"  Last Updated: {data['last_updated']}")

    checkpoint.close()


def verify_checksums(
    pipeline_name: str,
    scope: str = "file",
    limit: int = 100
) -> None:
    """Verify checksums for completed checkpoints.

    Args:
        pipeline_name: Name of the pipeline
        scope: Checkpoint scope to verify
        limit: Maximum number of checkpoints to verify
    """
    logger.info(f"Verifying checksums for pipeline: {pipeline_name}")

    checkpoint = PipelineCheckpoint(pipeline_name=pipeline_name)

    scope_enum = CheckpointScope(scope)
    completed = checkpoint.get_completed(scope=scope_enum)[:limit]

    if not completed:
        logger.info("No completed checkpoints to verify")
        checkpoint.close()
        return

    logger.info(f"Verifying {len(completed)} checkpoint(s)...\n")

    mismatches = []

    for i, item in enumerate(completed, 1):
        file_path = item['file_path']
        if not file_path:
            continue

        print(f"[{i}/{len(completed)}] Verifying {file_path}...", end=" ")

        if checkpoint.is_completed(
            scope_enum,
            item['scope_key'],
            file_path,
            verify_checksum=True
        ):
            print("OK")
        else:
            print("MISMATCH")
            mismatches.append(file_path)

    print()

    if mismatches:
        logger.warning(f"Found {len(mismatches)} checksum mismatches:")
        for path in mismatches:
            logger.warning(f"  - {path}")
        logger.info("\nTo re-process these files, clear their checkpoints and run the pipeline again")
    else:
        logger.info("All checksums verified successfully")

    checkpoint.close()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Recovery tool for OSAA data pipeline operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List failed operations
  %(prog)s list-failed --pipeline ingest

  # List completed operations
  %(prog)s list-completed --pipeline ingest --limit 50

  # Retry a specific file
  %(prog)s retry --pipeline ingest --scope file --key "s3://bucket/file.parquet"

  # Clear all failed checkpoints
  %(prog)s clear --pipeline ingest --status failed --confirm

  # Show statistics
  %(prog)s stats --pipeline ingest

  # Verify checksums
  %(prog)s verify --pipeline ingest --limit 100
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # List failed command
    list_failed_parser = subparsers.add_parser(
        "list-failed",
        help="List failed or pending operations"
    )
    list_failed_parser.add_argument(
        "--pipeline",
        required=True,
        help="Pipeline name (e.g., ingest, s3_sync, s3_promote)"
    )
    list_failed_parser.add_argument(
        "--scope",
        choices=["pipeline", "model", "file", "operation"],
        help="Filter by checkpoint scope"
    )

    # List completed command
    list_completed_parser = subparsers.add_parser(
        "list-completed",
        help="List completed operations"
    )
    list_completed_parser.add_argument(
        "--pipeline",
        required=True,
        help="Pipeline name"
    )
    list_completed_parser.add_argument(
        "--scope",
        choices=["pipeline", "model", "file", "operation"],
        help="Filter by checkpoint scope"
    )
    list_completed_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of results (default: 20)"
    )

    # Retry command
    retry_parser = subparsers.add_parser(
        "retry",
        help="Retry a specific operation"
    )
    retry_parser.add_argument("--pipeline", required=True, help="Pipeline name")
    retry_parser.add_argument(
        "--scope",
        required=True,
        choices=["pipeline", "model", "file", "operation"],
        help="Checkpoint scope"
    )
    retry_parser.add_argument("--key", required=True, help="Scope key identifier")
    retry_parser.add_argument("--file", help="Optional file path")

    # Clear command
    clear_parser = subparsers.add_parser(
        "clear",
        help="Clear checkpoints"
    )
    clear_parser.add_argument("--pipeline", required=True, help="Pipeline name")
    clear_parser.add_argument(
        "--scope",
        choices=["pipeline", "model", "file", "operation"],
        help="Filter by checkpoint scope"
    )
    clear_parser.add_argument("--key", help="Filter by scope key")
    clear_parser.add_argument(
        "--status",
        choices=["pending", "in_progress", "completed", "failed"],
        help="Filter by status"
    )
    clear_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt"
    )

    # Stats command
    stats_parser = subparsers.add_parser(
        "stats",
        help="Show checkpoint statistics"
    )
    stats_parser.add_argument("--pipeline", required=True, help="Pipeline name")

    # Verify command
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify checksums for completed checkpoints"
    )
    verify_parser.add_argument("--pipeline", required=True, help="Pipeline name")
    verify_parser.add_argument(
        "--scope",
        default="file",
        choices=["pipeline", "model", "file", "operation"],
        help="Checkpoint scope to verify (default: file)"
    )
    verify_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of checkpoints to verify (default: 100)"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    try:
        if args.command == "list-failed":
            list_failed_operations(args.pipeline, args.scope)

        elif args.command == "list-completed":
            list_completed_operations(args.pipeline, args.scope, args.limit)

        elif args.command == "retry":
            retry_operation(
                args.pipeline,
                args.scope,
                args.key,
                getattr(args, 'file', None)
            )

        elif args.command == "clear":
            clear_checkpoints(
                args.pipeline,
                args.scope,
                getattr(args, 'key', None),
                args.status,
                args.confirm
            )

        elif args.command == "stats":
            show_statistics(args.pipeline)

        elif args.command == "verify":
            verify_checksums(args.pipeline, args.scope, args.limit)

        return 0

    except Exception as e:
        logger.error(f"Command failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
