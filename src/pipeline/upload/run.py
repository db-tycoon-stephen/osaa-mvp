import duckdb
from pipeline.logging_config import create_logger
from pipeline.utils import s3_init
import pipeline.config as config

# Setup
logger = create_logger()

class Upload:
    def __init__(self):
        """
        Initialize the IngestProcess with S3 session and DuckDB connection.
        """
        self.s3_client, self.session = s3_init(return_session=True)
        self.con = duckdb.connect(config.DB_PATH)
        self.env = config.TARGET

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

    def get_sqlmesh_models(self):
        """
        Dynamically discover SQLMesh models across all schemas.
        
        :return: List of tuples (schema, table) for SQLMesh models
        """
        try:
            # Query to find all tables in schemas that look like SQLMesh managed schemas
            schemas_query = """
            SELECT DISTINCT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name LIKE '%__dev' OR schema_name LIKE '%__prod'
            """
            schemas = self.con.execute(schemas_query).fetchall()
            
            models = []
            for (schema,) in schemas:
                # Query tables in each schema
                tables_query = f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}'
                """
                tables = self.con.execute(tables_query).fetchall()
                
                # Add each table to models list
                models.extend([(schema, table[0]) for table in tables])
            
            logger.info(f"Discovered {len(models)} SQLMesh models")
            return models
        
        except Exception as e:
            logger.error(f"Error discovering SQLMesh models: {e}")
            raise

    def upload(self, schema_name: str, table_name: str, s3_file_path: str):
        """
        Upload a Duckdb table to s3, given the schema and table name and path.
        """    
        # Format the fully qualified table name with environment
        if self.env == 'prod':
            fully_qualified_name = f"{schema_name}.{table_name}"
        else:
            fully_qualified_name = f"{schema_name}__{self.env}.{table_name}"
        
        # Modify the query to use the correct table name
        self.con.sql(f"""
            COPY (SELECT * FROM {schema_name}.{table_name})
            TO '{s3_file_path}'
            (FORMAT 'parquet', OVERWRITE_OR_IGNORE 1);
        """)
        logger.info(f"Successfully uploaded {fully_qualified_name} to {s3_file_path}")

    def run(self):
        """
        Execute the full upload process.
        """
        try:
            # Set up S3 secret in DuckDB
            self.setup_s3_secret()

            # Dynamically get SQLMesh models
            sqlmesh_models = self.get_sqlmesh_models()

            # Define upload targets dynamically
            upload_targets = []
            for schema, table in sqlmesh_models:
                # Determine the category (edu or wdi) based on the table name or schema
                category = 'edu' if 'edu' in schema.lower() else 'wdi'
                
                # Construct S3 path
                s3_path = f's3://{config.S3_BUCKET_NAME}/{config.TRANSFORMED_AREA_FOLDER}/{category}/{table}.parquet'
                
                upload_targets.append((schema, table, s3_path))

            # Execute uploads
            for schema, table, s3_path in upload_targets:
                self.upload(schema, table, s3_path)

            logger.info("Upload process completed successfully.")
            logger.info(f"Uploaded {len(upload_targets)} models to S3")

        except Exception as e:
            logger.error(f"Upload process failed: {e}")
            raise

if __name__ == '__main__':
    upload_process = Upload()
    upload_process.run()