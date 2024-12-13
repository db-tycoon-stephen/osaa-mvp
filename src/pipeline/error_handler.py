"""
Global error handling and logging utilities for the OSAA MVP Pipeline.
"""

import sys
import traceback

from pipeline.logging_config import create_logger, log_exception

# Create a logger for this module
logger = create_logger(__name__)


def global_exception_handler(exc_type, exc_value, exc_traceback):
    """
    Global exception handler to log unhandled exceptions.

    :param exc_type: Exception type
    :param exc_value: Exception value
    :param exc_traceback: Exception traceback
    """
    # Log the full traceback
    logger.critical(" UNHANDLED EXCEPTION ")
    logger.critical("An unexpected error occurred that was not caught by local exception handlers.")

    # Format traceback
    traceback_details = traceback.extract_tb(exc_traceback)
    last_frame = traceback_details[-1]

    logger.critical(f"Error Location: {last_frame.filename}:{last_frame.lineno}")
    logger.critical(f"Function: {last_frame.name}")

    # Detailed error information
    logger.critical(f"Error Type: {exc_type.__name__}")
    logger.critical(f"Error Message: {exc_value}")

    # Troubleshooting guidance
    logger.critical("Troubleshooting Recommendations:")
    logger.critical("  1. Review recent code changes")
    logger.critical("  2. Check input data and configuration")
    logger.critical("  3. Verify system dependencies")
    logger.critical("  4. Consult project documentation")

    # Optional: You could add more specific handling based on error type

    # Ensure the error is still raised after logging
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


# Set the global exception handler
sys.excepthook = global_exception_handler
