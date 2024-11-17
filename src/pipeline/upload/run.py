
import duckdb
from pipeline.utils import setup_logger, s3_init
import pipeline.config as config

# Setup
logger = setup_logger(__name__)

class Upload:
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

    def upload(self, table_name: str, s3_file_path: str):
        """
        Upload a Duckdb table to s3, given the table name and path.
        """    

        logger.info(f"Uploading created table {table_name}")
        
        self.con.sql(f"""
            COPY (SELECT * FROM {table_name})
            TO '{s3_file_path}'
            (FORMAT PARQUET)
            """
        )

        logger.info(f"Uploading created table {table_name}")
        
    def run(self):
        """
        Run the entire upload process.
        """
        try:
            self.setup_s3_secret()
            self.upload(
                "intermediate.wdi",
                f's3://{config.S3_BUCKET_NAME}/{config.TRANSFORMED_AREA_FOLDER}/wdi/wdi_transformed.parquet'

            )
        finally:
            self.con.close()

if __name__ == '__main__':
    upload_process = Upload()
    upload_process.run()