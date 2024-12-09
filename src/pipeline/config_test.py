"""Test module for configuration settings and utilities.

This module contains unit tests for configuration-related
functions and utilities in the United Nations OSAA MVP project.
"""

import logging

import pipeline.config as config

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Log the paths from the config
logger.info(f"ROOT_DIR: {config.ROOT_DIR}")
logger.info(f"DATALAKE_DIR: {config.DATALAKE_DIR}")
logger.info(f"RAW_DATA_DIR: {config.RAW_DATA_DIR}")
logger.info(f"STAGING_DATA_DIR: {config.STAGING_DATA_DIR}")
logger.info(f"MASTER_DATA_DIR: {config.MASTER_DATA_DIR}")

logger.info(f"S3_BUCKET_NAME: {config.S3_BUCKET_NAME}")
logger.info(f"LANDING_AREA_FOLDER: {config.LANDING_AREA_FOLDER}")
logger.info(f"STAGING_AREA_PATH: {config.STAGING_AREA_PATH}")
