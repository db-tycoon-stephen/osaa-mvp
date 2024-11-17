import os

# get the local root directory 
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))



# Define the LOCAL DATA directory relative to the root
    # RAW_DATA_DIR = os.path.join(ROOT_DIR, 'raw_data')
    # PROC_DATA_DIR = os.path.join(ROOT_DIR, 'processed')

DATALAKE_DIR = os.path.join(ROOT_DIR, 'datalake')
RAW_DATA_DIR = os.path.join(DATALAKE_DIR, 'raw')
STAGING_DATA_DIR = os.path.join(DATALAKE_DIR, 'staging')
MASTER_DATA_DIR = os.path.join(STAGING_DATA_DIR, 'master')

# Allow both Docker and local environment DuckDB path
DB_PATH = os.getenv('DB_PATH', os.path.join(ROOT_DIR, 'sqlMesh', 'osaa_mvp.db'))

# Environment configurations
TARGET = os.getenv('TARGET', 'dev')  # dev, int, prod
USERNAME = os.getenv('USER', 'default')

# Adjust S3 environment path based on target
S3_ENV = USERNAME if TARGET == 'dev' else TARGET

# S3 configurations with environment-based paths
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'osaa-poc')
LANDING_AREA_FOLDER = f'{S3_ENV}/landing'
TRANSFORMED_AREA_FOLDER = f'{S3_ENV}/transformed'
STAGING_AREA_PATH = f'{S3_ENV}/staging'

# Local copy of master data
LOCAL=True
