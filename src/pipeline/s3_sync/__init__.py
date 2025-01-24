"""S3 sync package for SQLMesh database file management.

This package handles synchronization of SQLMesh database files with S3,
including downloading and uploading operations.
"""

import logging

logger = logging.getLogger(__name__)

def init_s3_sync_package() -> None:
    """Initialize the s3_sync package and log package details."""
    logger.info("🔄 Initializing OSAA MVP S3 Sync Package")
    logger.info("   📦 Package responsible for SQLMesh DB file sync")
    logger.info("   🔍 Ready to sync with S3")

# Call initialization when the package is imported
init_s3_sync_package()