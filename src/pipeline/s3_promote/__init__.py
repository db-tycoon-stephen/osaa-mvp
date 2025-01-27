"""S3 promote package for environment promotion.

This package handles the promotion of data between different S3 environments
(e.g., dev to prod) in the United Nations OSAA MVP project.
"""

import logging

logger = logging.getLogger(__name__)

def init_s3_promote_package() -> None:
    """Initialize the s3_promote package and log package details."""
    logger.info("🔄 Initializing OSAA MVP S3 Promote Package")
    logger.info("   📦 Package responsible for environment promotion")
    logger.info("   🔍 Ready to promote between environments")

# Call initialization when the package is imported
init_s3_promote_package()