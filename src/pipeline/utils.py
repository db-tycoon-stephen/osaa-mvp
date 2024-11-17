import os
import logging
import boto3

### LOGGER ###
def setup_logger(script_name: str = None) -> logging.Logger:
    """
    Set up a logger with a basic configuration and optional script/module name.

    :param script_name: Name of the script or module (if None, uses the calling module's __name__).
    :return: Configured logger
    """
    if script_name is None:
        script_name = __name__

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)

    # Create handler (streaming to console)
    handler = logging.StreamHandler()

    # Define format including script/module name
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

logger = setup_logger()

### s3 INITIALIZER ###
def s3_init(return_session=False) -> boto3.client:
    """
    Initialize and return an S3 client using credentials from environment variables.

    :return: boto3 S3 client object
    """
    from dotenv import load_dotenv
    load_dotenv()

    try:
        # Try environment variables first (makes logic compatible with just-based local runs and docker)
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID') or os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.environ.get('AWS_DEFAULT_REGION') or os.getenv('AWS_DEFAULT_REGION')

        if not all([aws_access_key, aws_secret_key, aws_region]):
            raise ValueError("Missing required AWS credentials")

        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        s3_client = session.client('s3')

        logger.info("S3 client initialized successfully.")
        
        if return_session:
            return s3_client, session
        else:
            return s3_client
    
    except Exception as e:
        logger.error(f"Error initializing S3 client: {e}")
        raise

### AWS S3 INTERACTIONS ###
def get_s3_file_paths(bucket_name: str, prefix: str) -> dict:
    """
    Get a list of file paths from the S3 bucket and organize them into a dictionary.

    :param bucket_name: The name of the S3 bucket.
    :param prefix: The folder prefix to filter the file paths.
    :return: Dictionary of file paths organized by source folder.
    """
    try:
        s3_prefix = prefix + '/'

        paginator = s3_init().get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_prefix}
        page_iterator = paginator.paginate(**operation_parameters)
        filtered_iterator = page_iterator.search(f"Contents[?Key != '{s3_prefix}'][]")

        file_paths = {}
        for key_data in filtered_iterator:
            key = key_data['Key']
            parts = key.split('/')
            if len(parts) >= 4:  # New structure: [env, landing, source, filename]
                source, filename = parts[-2], parts[-1]  # Take the last two parts
                if source not in file_paths:
                    file_paths[source] = {}
                file_paths[source][filename.split('.')[0]] = f"s3://{bucket_name}/{key}"

        logger.info(f"Successfully retrieved file paths from S3 bucket {bucket_name}.")
        return file_paths
    except Exception as e:
        logger.error(f"Error retrieving file paths from S3: {e}")
        raise

def download_s3_client(s3_client: boto3.client, s3_bucket_name: str, s3_folder: str, local_dir: str) -> None:
    """
    Download all files from a specified S3 folder to a local directory.

    :param s3_client: The boto3 S3 client.
    :param s3_bucket_name: The name of the S3 bucket.
    :param s3_folder: The folder within the S3 bucket to download files from.
    :param local_dir: The local directory to save downloaded files.
    """
    try:
        # Ensure the local directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # List objects in the specified S3 folder
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_folder)
        
        # Check if any objects are returned
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_key = obj['Key']
                filename = s3_key.split('/')[-1]  # Take only the last part as filename
                local_file_path = os.path.join(local_dir, filename)
                
                # Download the file
                s3_client.download_file(s3_bucket_name, s3_key, local_file_path)
                logger.info(f'Successfully downloaded {s3_key} to {local_file_path}')
        else:
            logger.warning(f'No files found in s3://{s3_bucket_name}/{s3_folder}')

    except Exception as e:
        logger.error(f'Error downloading files from S3: {e}', exc_info=True)
        raise