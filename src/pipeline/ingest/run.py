import os
import re
import duckdb
from pipeline.utils import setup_logger, s3_init
import pipeline.config as config

# Setup
logger = setup_logger(__name__)

class Ingest:
    def __init__(self):
        """
        Initialize the IngestProcess with S3 session and DuckDB connection.
        """
        self.s3_client, self.session = s3_init(return_session=True)
        self.con = duckdb.connect(config.DB_PATH)

    def setup_s3_secret(self):
        """
        Set up the S3 secret in DuckDB for S3 access.
        """
        try:
            region = self.session.region_name
            credentials = self.session.get_credentials().get_frozen_credentials()

            self.con.sql(f"""
            CREATE SECRET my_s3_secret (
                TYPE S3,
                KEY_ID '{credentials.access_key}',
                SECRET '{credentials.secret_key}',
                REGION '{region}'
            );
            """)
            logger.info("S3 secret setup in DuckDB.")

        except Exception as e:
            logger.error(f"Error setting up S3 secret: {e}")
            raise

    def convert_csv_to_parquet_and_upload(self, local_file_path: str, s3_file_path: str):
        """
        Convert a CSV file to Parquet and upload it to S3.

        :param local_file_path: Path to the local CSV file.
        :param s3_file_path: The S3 file path for the output Parquet file.
        """
        try:
            table_name = re.search(r'[^/]+(?=\.)', local_file_path)
            table_name = table_name.group(0).replace('-','_') if table_name else "UNNAMED"
            fully_qualified_name = 'source.' + table_name

            self.con.sql("CREATE SCHEMA IF NOT EXISTS source")

            self.con.sql(f"drop table if exists {fully_qualified_name}")
            self.con.sql(f"""
                CREATE TABLE {fully_qualified_name} AS
                SELECT * 
                FROM read_csv('{local_file_path}', header = true)
                """           
            )

            logger.info(f"Successfully created table {fully_qualified_name}")
            
            self.con.sql(f"""
                COPY (SELECT * FROM {fully_qualified_name})
                TO '{s3_file_path}'
                (FORMAT PARQUET)
                """
            )

            logger.info(f"Successfully converted and uploaded {local_file_path} to {s3_file_path}")

        except Exception as e:
            logger.error(f"Error converting and uploading {local_file_path} to S3: {e}", exc_info=True)
            raise

    def generate_file_to_s3_folder_mapping(self, raw_data_dir: str) -> dict:
        """
        Generate mapping of local files to their respective S3 folders. Excludes any folder and file that starts with symbols.

        :param raw_data_dir: The base directory containing raw data subfolders.
        :return: A dictionary where the key is the filename and the value is the subfolder name.
        """
        file_to_s3_folder_mapping = {}

        # Pattern to exclude any folder/file name with symbols (like hidden files)
        symbols = r"^[~!@#$%^&*()_\-+={[}}|:;\"'<,>.?/]+"


        # Traverse the raw_data directory
        for subdir, _, files in os.walk(raw_data_dir):
            logger.info(f"Walking directory: {subdir}, found files: {files}")
            # Get the subfolder name (relative to raw_data_dir)
            sub_folder = os.path.relpath(subdir, raw_data_dir)

            # Exclude folders that start with any symbol in the excluded set
            if re.match(symbols, sub_folder):
                logger.info(f"Skipping folder: {sub_folder} due to symbols.")
                continue

            # Map each file to its corresponding subfolder, but exclude files starting with symbols
            for file_name in files:
                if not re.match(symbols, file_name):
                    logger.info(f"Mapping file: {file_name} in subfolder {sub_folder}")
                    file_to_s3_folder_mapping[file_name] = sub_folder

        logger.info(f"Generated file mapping: {file_to_s3_folder_mapping}")
        return file_to_s3_folder_mapping


    def convert_and_upload_files(self):
        """
        Convert CSV files to Parquet and upload them to S3.
        """
        try:
            file_mapping = self.generate_file_to_s3_folder_mapping(config.RAW_DATA_DIR)
            for file_name_csv, s3_sub_folder in file_mapping.items():

                local_file_path = os.path.join(config.RAW_DATA_DIR, s3_sub_folder, file_name_csv)

                file_name_pq = f'{os.path.splitext(file_name_csv)[0]}.parquet'

                s3_file_path = f's3://{config.S3_BUCKET_NAME}/{config.LANDING_AREA_FOLDER}/{s3_sub_folder}/{file_name_pq}'

                logger.info(f"Processing local file: {local_file_path}")
                logger.info(f"Uploading to S3: {s3_file_path}")

                if os.path.isfile(local_file_path):
                    self.convert_csv_to_parquet_and_upload(local_file_path, s3_file_path)
                else:
                    logger.warning(f'File not found: {local_file_path}')
            logger.info("Ingestion process completed successfully.")
            
        except Exception as e:
            logger.error(f"Error during file ingestion: {e}")
            raise

    def run(self):
        """
        Run the entire ingestion process.
        """
        try:
            self.setup_s3_secret()
            self.convert_and_upload_files()
        finally:
            self.con.close()

if __name__ == '__main__':
    ingest_process = Ingest()
    ingest_process.run()