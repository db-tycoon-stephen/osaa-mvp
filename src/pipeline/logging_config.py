"""
Centralized logging configuration for the OSAA MVP Pipeline.

This module provides a consistent, structured logging approach 
across the entire project.
"""

import logging
import colorlog
import sys

def create_logger(name=None):
    """
    Create a structured, color-coded logger with clean output.
    
    :param name: Name of the logger (typically __name__)
    :return: Configured logger instance
    """
    # Create logger
    logger = colorlog.getLogger(name or __name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler
    console_handler = colorlog.StreamHandler(sys.stdout)
    
    # Custom log format with clear structure
    formatter = colorlog.ColoredFormatter(
        # Structured format with clear sections
        '%(log_color)s[%(levelname)s]%(reset)s '
        '%(blue)s[%(name)s]%(reset)s '
        '%(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white'
        },
        secondary_log_colors={}
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def log_exception(logger, e, context=None):
    """
    Standardized exception logging with optional context.
    
    :param logger: Logger instance
    :param e: Exception object
    :param context: Optional additional context for the error
    """
    logger.critical('🚨 UNEXPECTED ERROR 🚨')
    logger.critical(f'Error Type: {type(e).__name__}')
    logger.critical(f'Error Details: {str(e)}')
    
    if context:
        logger.critical(f'Context: {context}')
    
    # Optional: Add troubleshooting steps or recommendations
    logger.critical('Troubleshooting:')
    logger.critical('  1. Check recent changes')
    logger.critical('  2. Verify input data')
    logger.critical('  3. Review system logs')
    logger.critical('  4. Consult project documentation')

# Global logger for use in modules that don't specify a name
default_logger = create_logger()
