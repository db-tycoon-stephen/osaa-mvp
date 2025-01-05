#!/usr/bin/env python3

import os
import boto3
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import json
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def create_test_parquet():
    """Create a test Parquet file with sample data"""
    # Create sample data
    data = {
        'id': range(1, 6),
        'name': ['Test1', 'Test2', 'Test3', 'Test4', 'Test5'],
        'value': [10.5, 20.0, 30.7, 40.2, 50.9],
        'timestamp': [datetime.now()] * 5
    }
    df = pd.DataFrame(data)
    
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    
    # Write DataFrame to Parquet file
    df.to_parquet(temp_file.name, engine='pyarrow')
    
    return temp_file.name

def test_initial_credentials():
    """Test the initial AWS credentials before role assumption"""
    try:
        # Get credentials from environment
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        role_arn = os.getenv('AWS_ROLE_ARN')

        logger.info("Testing initial credentials...")
        logger.info(f"Access Key ID: {'*' * 16 + access_key[-4:] if access_key else 'Not Set'}")
        logger.info(f"Secret Key: {'*' * 36 + secret_key[-4:] if secret_key else 'Not Set'}")
        logger.info(f"Region: {region}")
        logger.info(f"Role ARN: {role_arn}")

        # Create an STS client with initial credentials
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )

        # Test the credentials by getting the caller identity
        identity = sts_client.get_caller_identity()
        logger.info(f"Successfully authenticated as: {identity['Arn']}")
        
        return sts_client, role_arn

    except Exception as e:
        logger.error(f"Error testing initial credentials: {e}")
        raise

def test_role_assumption(sts_client, role_arn):
    """Test assuming the specified role"""
    try:
        logger.info(f"Attempting to assume role: {role_arn}")
        
        # Assume the role
        assumed_role = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName='TestSession'
        )
        
        logger.info("Successfully assumed role")
        logger.info(f"Access Key ID: {'*' * 16 + assumed_role['Credentials']['AccessKeyId'][-4:]}")
        logger.info(f"Expiration: {assumed_role['Credentials']['Expiration']}")
        
        return assumed_role['Credentials']

    except ClientError as e:
        logger.error(f"Error assuming role: {e}")
        raise

def test_s3_access(credentials):
    """Test S3 access with the assumed role credentials"""
    try:
        # Create an S3 client with the assumed role credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )

        # Test listing buckets
        logger.info("Testing S3 access by listing buckets...")
        response = s3_client.list_buckets()
        
        logger.info("Successfully listed buckets:")
        for bucket in response['Buckets']:
            logger.info(f"- {bucket['Name']}")

        # Test specific bucket access
        bucket_name = os.getenv('S3_BUCKET_NAME', 'unosaa-data-pipeline')
        target = os.getenv('TARGET', 'dev')
        username = os.getenv('USERNAME', 'test-user')
        
        logger.info(f"\nTesting access to specific bucket: {bucket_name}")
        
        # Construct S3 path according to our project structure
        if target == "prod":
            base_path = "landing/test"
        else:
            base_path = f"dev/{target}_{username}/landing/test"
        
        # Try to list objects in the bucket
        logger.info(f"Listing objects in bucket with prefix: {base_path}")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=base_path,
            MaxKeys=5  # Limit to 5 objects for testing
        )
        
        if 'Contents' in response:
            logger.info("Found existing objects in bucket:")
            for obj in response['Contents']:
                logger.info(f"- {obj['Key']}")
        else:
            logger.info(f"No existing objects found with prefix: {base_path}")

        # Test writing to S3
        logger.info("\nTesting S3 write access...")
        test_data = {
            'test': 'data',
            'timestamp': datetime.now().isoformat()
        }
        test_key = f"{base_path}/credentials_test.json"
        
        logger.info(f"Attempting to write test file to s3://{bucket_name}/{test_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=json.dumps(test_data)
        )
        logger.info("Successfully wrote test file to S3")

        # Verify the file was written
        logger.info("Verifying test file exists...")
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=test_key
        )
        logger.info("Successfully verified test file exists")
        
        # Read back the contents
        data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"File contents: {data}")

        # Test Parquet file upload
        logger.info("\nTesting Parquet file upload...")
        parquet_file = create_test_parquet()
        parquet_key = f"{base_path}/test_data.parquet"
        
        logger.info(f"Uploading Parquet file to s3://{bucket_name}/{parquet_key}")
        with open(parquet_file, 'rb') as f:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=parquet_key,
                Body=f
            )
        logger.info("Successfully uploaded Parquet file")
        
        # Clean up temporary file
        os.unlink(parquet_file)
        
        # Verify the Parquet file was uploaded
        logger.info("Verifying Parquet file exists...")
        response = s3_client.head_object(
            Bucket=bucket_name,
            Key=parquet_key
        )
        logger.info(f"Successfully verified Parquet file exists (size: {response['ContentLength']} bytes)")

    except ClientError as e:
        logger.error(f"Error testing S3 access: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def main():
    """Main test function"""
    try:
        logger.info("Starting AWS credentials and S3 access test...")
        
        # Test initial credentials and get STS client
        sts_client, role_arn = test_initial_credentials()
        
        # Test role assumption
        assumed_credentials = test_role_assumption(sts_client, role_arn)
        
        # Test S3 access with assumed role
        test_s3_access(assumed_credentials)
        
        logger.info("\nAll tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    main() 