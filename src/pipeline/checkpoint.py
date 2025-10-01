"""Checkpoint system for tracking pipeline progress and enabling idempotent operations.

This module provides a DuckDB-based checkpoint system that tracks processed files,
their checksums, and processing status. It enables safe retry logic and ensures
idempotent file operations across pipeline runs.

Key features:
- Track processed files with checksums
- Support for different checkpoint scopes (pipeline, model, file)
- Query processed and pending files
- Clear checkpoints for retry scenarios
- Atomic checkpoint operations
"""

import hashlib
import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import duckdb

from pipeline.logging_config import create_logger

logger = create_logger(__name__)


class CheckpointStatus(str, Enum):
    """Enumeration of possible checkpoint statuses."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class CheckpointScope(str, Enum):
    """Enumeration of checkpoint scope levels."""
    PIPELINE = "pipeline"
    MODEL = "model"
    FILE = "file"
    OPERATION = "operation"


class PipelineCheckpoint:
    """Manages checkpoint state for pipeline operations.

    This class provides methods to track file processing state, enabling
    idempotent operations and recovery from failures. It uses DuckDB for
    persistent storage of checkpoint information.

    Attributes:
        db_path: Path to the DuckDB checkpoint database
        pipeline_name: Name of the pipeline for scoping checkpoints
        con: DuckDB connection object
    """

    def __init__(
        self,
        pipeline_name: str,
        db_path: Optional[str] = None
    ):
        """Initialize the checkpoint manager.

        Args:
            pipeline_name: Name of the pipeline for scoping checkpoints
            db_path: Path to checkpoint database (defaults to .checkpoints/)
        """
        self.pipeline_name = pipeline_name

        # Default to .checkpoints directory in project root
        if db_path is None:
            checkpoint_dir = Path(".checkpoints")
            checkpoint_dir.mkdir(exist_ok=True)
            db_path = str(checkpoint_dir / f"{pipeline_name}_checkpoint.db")

        self.db_path = db_path
        logger.info(f"Initializing checkpoint system at {self.db_path}")

        # Initialize DuckDB connection
        self.con = duckdb.connect(self.db_path)
        self._initialize_tables()

    def _initialize_tables(self) -> None:
        """Create checkpoint tables if they don't exist."""
        logger.debug("Initializing checkpoint tables")

        # Main checkpoint table
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_id VARCHAR PRIMARY KEY,
                pipeline_name VARCHAR NOT NULL,
                scope VARCHAR NOT NULL,
                scope_key VARCHAR NOT NULL,
                file_path VARCHAR,
                checksum VARCHAR,
                status VARCHAR NOT NULL,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                failed_at TIMESTAMP,
                error_message VARCHAR,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for efficient queries
        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_pipeline_status
            ON checkpoints(pipeline_name, status)
        """)

        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_scope_key
            ON checkpoints(scope, scope_key)
        """)

        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_checksum
            ON checkpoints(file_path, checksum)
        """)

        logger.info("Checkpoint tables initialized successfully")

    def _generate_checkpoint_id(
        self,
        scope: CheckpointScope,
        scope_key: str,
        file_path: Optional[str] = None
    ) -> str:
        """Generate a unique checkpoint ID.

        Args:
            scope: Checkpoint scope level
            scope_key: Key identifying the scope (e.g., model name)
            file_path: Optional file path for file-level checkpoints

        Returns:
            Unique checkpoint ID
        """
        parts = [self.pipeline_name, scope.value, scope_key]
        if file_path:
            parts.append(file_path)

        id_string = "|".join(parts)
        return hashlib.md5(id_string.encode()).hexdigest()

    def calculate_file_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum of a file.

        Args:
            file_path: Path to the file

        Returns:
            MD5 checksum as hex string

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            # Read in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)

        return md5_hash.hexdigest()

    def mark_started(
        self,
        scope: CheckpointScope,
        scope_key: str,
        file_path: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """Mark a checkpoint as started.

        Args:
            scope: Checkpoint scope level
            scope_key: Key identifying the scope
            file_path: Optional file path for file-level checkpoints
            metadata: Optional metadata dictionary

        Returns:
            Checkpoint ID
        """
        checkpoint_id = self._generate_checkpoint_id(scope, scope_key, file_path)

        # Calculate checksum if file path provided
        checksum = None
        if file_path and os.path.exists(file_path):
            try:
                checksum = self.calculate_file_checksum(file_path)
            except Exception as e:
                logger.warning(f"Could not calculate checksum for {file_path}: {e}")

        # Check if checkpoint already exists
        existing = self.con.execute("""
            SELECT status, checksum FROM checkpoints
            WHERE checkpoint_id = ?
        """, [checkpoint_id]).fetchone()

        if existing:
            status, old_checksum = existing

            # If completed and checksum matches, skip
            if status == CheckpointStatus.COMPLETED.value and checksum == old_checksum:
                logger.info(f"Checkpoint {checkpoint_id} already completed with matching checksum")
                return checkpoint_id

            # Update existing checkpoint
            logger.debug(f"Updating existing checkpoint {checkpoint_id}")
            self.con.execute("""
                UPDATE checkpoints
                SET status = ?,
                    started_at = ?,
                    completed_at = NULL,
                    failed_at = NULL,
                    error_message = NULL,
                    checksum = ?,
                    metadata = ?,
                    updated_at = ?
                WHERE checkpoint_id = ?
            """, [
                CheckpointStatus.IN_PROGRESS.value,
                datetime.now(),
                checksum,
                str(metadata) if metadata else None,
                datetime.now(),
                checkpoint_id
            ])
        else:
            # Insert new checkpoint
            logger.debug(f"Creating new checkpoint {checkpoint_id}")
            self.con.execute("""
                INSERT INTO checkpoints (
                    checkpoint_id, pipeline_name, scope, scope_key,
                    file_path, checksum, status, started_at, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                checkpoint_id,
                self.pipeline_name,
                scope.value,
                scope_key,
                file_path,
                checksum,
                CheckpointStatus.IN_PROGRESS.value,
                datetime.now(),
                str(metadata) if metadata else None
            ])

        logger.info(f"Marked checkpoint {checkpoint_id} as started")
        return checkpoint_id

    def mark_completed(
        self,
        scope: CheckpointScope,
        scope_key: str,
        file_path: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> None:
        """Mark a checkpoint as completed.

        Args:
            scope: Checkpoint scope level
            scope_key: Key identifying the scope
            file_path: Optional file path for file-level checkpoints
            metadata: Optional metadata dictionary
        """
        checkpoint_id = self._generate_checkpoint_id(scope, scope_key, file_path)

        # Update checksum if file exists
        checksum = None
        if file_path and os.path.exists(file_path):
            try:
                checksum = self.calculate_file_checksum(file_path)
            except Exception as e:
                logger.warning(f"Could not calculate checksum for {file_path}: {e}")

        self.con.execute("""
            UPDATE checkpoints
            SET status = ?,
                completed_at = ?,
                checksum = COALESCE(?, checksum),
                metadata = COALESCE(?, metadata),
                updated_at = ?
            WHERE checkpoint_id = ?
        """, [
            CheckpointStatus.COMPLETED.value,
            datetime.now(),
            checksum,
            str(metadata) if metadata else None,
            datetime.now(),
            checkpoint_id
        ])

        logger.info(f"Marked checkpoint {checkpoint_id} as completed")

    def mark_failed(
        self,
        scope: CheckpointScope,
        scope_key: str,
        error_message: str,
        file_path: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> None:
        """Mark a checkpoint as failed.

        Args:
            scope: Checkpoint scope level
            scope_key: Key identifying the scope
            error_message: Error message describing the failure
            file_path: Optional file path for file-level checkpoints
            metadata: Optional metadata dictionary
        """
        checkpoint_id = self._generate_checkpoint_id(scope, scope_key, file_path)

        self.con.execute("""
            UPDATE checkpoints
            SET status = ?,
                failed_at = ?,
                error_message = ?,
                metadata = COALESCE(?, metadata),
                updated_at = ?
            WHERE checkpoint_id = ?
        """, [
            CheckpointStatus.FAILED.value,
            datetime.now(),
            error_message[:1000],  # Truncate long error messages
            str(metadata) if metadata else None,
            datetime.now(),
            checkpoint_id
        ])

        logger.warning(f"Marked checkpoint {checkpoint_id} as failed: {error_message}")

    def is_completed(
        self,
        scope: CheckpointScope,
        scope_key: str,
        file_path: Optional[str] = None,
        verify_checksum: bool = True
    ) -> bool:
        """Check if a checkpoint is completed.

        Args:
            scope: Checkpoint scope level
            scope_key: Key identifying the scope
            file_path: Optional file path for file-level checkpoints
            verify_checksum: Whether to verify file checksum matches

        Returns:
            True if checkpoint is completed and checksum matches (if verified)
        """
        checkpoint_id = self._generate_checkpoint_id(scope, scope_key, file_path)

        result = self.con.execute("""
            SELECT status, checksum FROM checkpoints
            WHERE checkpoint_id = ?
        """, [checkpoint_id]).fetchone()

        if not result:
            return False

        status, stored_checksum = result

        # Check if status is completed
        if status != CheckpointStatus.COMPLETED.value:
            return False

        # Optionally verify checksum if file exists
        if verify_checksum and file_path and os.path.exists(file_path):
            try:
                current_checksum = self.calculate_file_checksum(file_path)
                if current_checksum != stored_checksum:
                    logger.info(
                        f"Checksum mismatch for {file_path}. "
                        f"Stored: {stored_checksum}, Current: {current_checksum}"
                    )
                    return False
            except Exception as e:
                logger.warning(f"Could not verify checksum for {file_path}: {e}")
                return False

        return True

    def get_status(
        self,
        scope: CheckpointScope,
        scope_key: str,
        file_path: Optional[str] = None
    ) -> Optional[CheckpointStatus]:
        """Get the status of a checkpoint.

        Args:
            scope: Checkpoint scope level
            scope_key: Key identifying the scope
            file_path: Optional file path for file-level checkpoints

        Returns:
            CheckpointStatus or None if not found
        """
        checkpoint_id = self._generate_checkpoint_id(scope, scope_key, file_path)

        result = self.con.execute("""
            SELECT status FROM checkpoints
            WHERE checkpoint_id = ?
        """, [checkpoint_id]).fetchone()

        if result:
            return CheckpointStatus(result[0])
        return None

    def get_pending(
        self,
        scope: Optional[CheckpointScope] = None,
        scope_key: Optional[str] = None
    ) -> List[Dict]:
        """Get all pending or failed checkpoints.

        Args:
            scope: Optional scope filter
            scope_key: Optional scope key filter

        Returns:
            List of checkpoint dictionaries
        """
        query = """
            SELECT checkpoint_id, scope, scope_key, file_path,
                   status, error_message, created_at, updated_at
            FROM checkpoints
            WHERE pipeline_name = ?
            AND status IN (?, ?)
        """

        params = [
            self.pipeline_name,
            CheckpointStatus.PENDING.value,
            CheckpointStatus.FAILED.value
        ]

        if scope:
            query += " AND scope = ?"
            params.append(scope.value)

        if scope_key:
            query += " AND scope_key = ?"
            params.append(scope_key)

        query += " ORDER BY created_at"

        results = self.con.execute(query, params).fetchall()

        return [
            {
                "checkpoint_id": r[0],
                "scope": r[1],
                "scope_key": r[2],
                "file_path": r[3],
                "status": r[4],
                "error_message": r[5],
                "created_at": r[6],
                "updated_at": r[7]
            }
            for r in results
        ]

    def get_completed(
        self,
        scope: Optional[CheckpointScope] = None,
        scope_key: Optional[str] = None
    ) -> List[Dict]:
        """Get all completed checkpoints.

        Args:
            scope: Optional scope filter
            scope_key: Optional scope key filter

        Returns:
            List of checkpoint dictionaries
        """
        query = """
            SELECT checkpoint_id, scope, scope_key, file_path,
                   checksum, completed_at
            FROM checkpoints
            WHERE pipeline_name = ?
            AND status = ?
        """

        params = [self.pipeline_name, CheckpointStatus.COMPLETED.value]

        if scope:
            query += " AND scope = ?"
            params.append(scope.value)

        if scope_key:
            query += " AND scope_key = ?"
            params.append(scope_key)

        query += " ORDER BY completed_at DESC"

        results = self.con.execute(query, params).fetchall()

        return [
            {
                "checkpoint_id": r[0],
                "scope": r[1],
                "scope_key": r[2],
                "file_path": r[3],
                "checksum": r[4],
                "completed_at": r[5]
            }
            for r in results
        ]

    def clear_checkpoint(
        self,
        scope: CheckpointScope,
        scope_key: str,
        file_path: Optional[str] = None
    ) -> None:
        """Clear a specific checkpoint.

        Args:
            scope: Checkpoint scope level
            scope_key: Key identifying the scope
            file_path: Optional file path for file-level checkpoints
        """
        checkpoint_id = self._generate_checkpoint_id(scope, scope_key, file_path)

        self.con.execute("""
            DELETE FROM checkpoints
            WHERE checkpoint_id = ?
        """, [checkpoint_id])

        logger.info(f"Cleared checkpoint {checkpoint_id}")

    def clear_all_checkpoints(
        self,
        scope: Optional[CheckpointScope] = None,
        scope_key: Optional[str] = None,
        status: Optional[CheckpointStatus] = None
    ) -> int:
        """Clear checkpoints matching the given filters.

        Args:
            scope: Optional scope filter
            scope_key: Optional scope key filter
            status: Optional status filter

        Returns:
            Number of checkpoints cleared
        """
        query = "DELETE FROM checkpoints WHERE pipeline_name = ?"
        params = [self.pipeline_name]

        if scope:
            query += " AND scope = ?"
            params.append(scope.value)

        if scope_key:
            query += " AND scope_key = ?"
            params.append(scope_key)

        if status:
            query += " AND status = ?"
            params.append(status.value)

        result = self.con.execute(query, params)
        count = result.fetchone()[0] if result else 0

        logger.info(f"Cleared {count} checkpoints")
        return count

    def get_statistics(self) -> Dict:
        """Get checkpoint statistics.

        Returns:
            Dictionary with checkpoint statistics
        """
        stats = self.con.execute("""
            SELECT
                status,
                COUNT(*) as count,
                MIN(created_at) as first_created,
                MAX(updated_at) as last_updated
            FROM checkpoints
            WHERE pipeline_name = ?
            GROUP BY status
        """, [self.pipeline_name]).fetchall()

        result = {
            "pipeline_name": self.pipeline_name,
            "by_status": {}
        }

        for status, count, first_created, last_updated in stats:
            result["by_status"][status] = {
                "count": count,
                "first_created": first_created,
                "last_updated": last_updated
            }

        return result

    def close(self) -> None:
        """Close the database connection."""
        if self.con:
            self.con.close()
            logger.debug("Checkpoint database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
