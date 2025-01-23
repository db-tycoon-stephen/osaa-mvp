import boto3
import pyarrow.parquet as pq
import io
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get the bucket name from the environment
bucket_name = os.getenv('S3_BUCKET_NAME', 'unosaa-data-pipeline')

# Define the key for the indicators model
key = 'prod/staging/master/indicators.parquet'

def read_parquet_from_s3(bucket_name, key):
    # Initialize a session using Amazon S3
    s3 = boto3.client('s3')

    # Get the object from the S3 bucket
    response = s3.get_object(Bucket=bucket_name, Key=key)

    # Read the Parquet file from the response
    parquet_file = pq.ParquetFile(io.BytesIO(response['Body'].read()))

    # Convert the Parquet file to a Pandas DataFrame
    table = parquet_file.read()
    df = table.to_pandas()

    # Get metadata from the S3 object
    last_modified = response['LastModified']
    logger.info(f"Last updated timestamp: {last_modified}")

    # Get the total row count
    row_count = table.num_rows
    logger.info(f"Total row count: {row_count}")

    # Display the DataFrame
    print(df)


def list_tables_in_s3(bucket_name, prefix=''):
    """List all tables (Parquet files) available in the specified S3 bucket and path."""
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        logger.info("Available tables (Parquet files):")
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                logger.info(f"- {obj['Key']}")
    else:
        logger.info("No tables found.")


def show_table_schema(bucket_name, key):
    """Show the schema for a given table (Parquet file)."""
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    parquet_file = pq.ParquetFile(io.BytesIO(response['Body'].read()))
    schema = parquet_file.schema
    logger.info(f"Schema for {key}:")
    logger.info(schema)


if __name__ == "__main__":
    # List all tables in the specified path
    list_tables_in_s3(bucket_name, 'prod/staging/master/')
    
    # Show schema for the indicators table
    show_table_schema(bucket_name, key)
    
    # Read and display the Parquet file
    read_parquet_from_s3(bucket_name, key) 