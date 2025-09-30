# OSAA Data Pipeline MVP

## 1. Purpose

This project implements a **Minimum Viable Product** (MVP) Data Pipeline for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging modern data engineering tools to create an efficient and scalable data processing system.

## 2. Quickstart

Here's how to get started with the OSAA Data Pipeline:

1. **Setup Environment**
   ```bash
   # Clone the repository
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp

   # Copy and configure environment variables (get from your team lead)
   cp .env.example .env
   ```

2. **Build the Container** (Required before first run and after code changes)
   ```bash
   # Build the Docker container - this may take a few minutes
   docker build -t osaa-mvp .
   ```

3. **Run the Pipeline**
   ```bash
   # Run the complete pipeline
   docker compose run --rm pipeline ingest
   docker compose run --rm pipeline etl
   ```

4. **Common Commands**
   ```bash
   # Run only data ingestion
   docker compose run --rm pipeline ingest

   # Run only transformations
   docker compose run --rm pipeline transform

   # Run a configuration test
   docker compose run --rm pipeline config_test

   # Promote data (from dev/landing to prod/landing)
   docker compose run --rm pipeline promote

   # Run in development mode with your username
   docker compose run --rm -e USERNAME=your_name pipeline etl
   ```

5. **View Results**
   - Processed data will be available in the S3 bucket
   - Source files: `s3://unosaa-data-pipeline/dev/landing/...`
   - Your development data: `s3://unosaa-data-pipeline/dev/dev_{USERNAME}/...`
   - Production data: `s3://unosaa-data-pipeline/prod/...`

For detailed instructions and advanced usage, see the sections below.

## 3. Getting Started

### 3.1 Required Software

- [Docker Desktop](https://www.docker.com/products/docker-desktop/): Available for Windows, Mac, and Linux
- [Git](https://git-scm.com/downloads): Choose the version for your operating system

After installing Docker Desktop, you'll need to start the application before running any pipeline commands.

### 3.2 Basic Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp
   ```

2. **Configure Environment**
   Copy the example configuration file:
   ```bash
   cp .env.example .env
   ```
   Get the required credentials from your team lead and update `.env`

3. **Build and Run**
   ```bash
   # IMPORTANT: Build the Docker container first
   docker build -t osaa-mvp .

   # Then run the pipeline
   docker compose run --rm pipeline etl
   ```

   Note: You must rebuild the container whenever you make changes to the code or when pulling updates from GitHub

### 3.3 Troubleshooting Common Issues

#### Docker Issues

1. **Docker Not Running**
   - Make sure Docker Desktop is running
   - Restart Docker Desktop if needed
   - Check system resources

2. **Network Issues**
   - Ensure system is connected to the internet
   - Check VPN status if required

#### Pipeline Issues

1. **Access Denied**
   - Verify your credentials in `.env`
   - Contact your team lead for valid credentials

2. **Data Not Found**
   - Check that your source data is in the correct location
   - Verify file names and formats

If issues persist, contact your team lead with detailed error information.

## 4. Working with Data

### 4.1 Running the Pipeline

The pipeline processes data in three main steps:
1. **Ingest**: Converts source data (CSV) to optimized format
2. **Transform**: Applies data transformations and cleaning
3. **Upload**: Stores results in the cloud

#### Basic Commands
```bash
# Run the complete pipeline
docker compose run --rm pipeline ingest
docker compose run --rm pipeline etl

# Run individual steps
docker compose run --rm pipeline ingest    # Only ingest new data
docker compose run --rm pipeline transform # Only run transformations
```

### 4.2 Adding New Data

To add a new dataset:

1. **Stage Your Data in the local upload folder**
   - Save your CSV file in `data/raw/<source_name>/`
   - Ensure the data follows the required format

2. **Intake the data into SQLMesh with a source model**
   - Create a source model for your new source in SQLMesh: `sqlMesh/models/sources/<source_name>/<source_model_name>.sql`

   Example:
   ```sql
   MODEL (
      name sdg.data_national,
      kind FULL,
      cron '@daily',
      columns (
         INDICATOR_ID TEXT,
         COUNTRY_ID TEXT,
         YEAR INTEGER,
         VALUE DECIMAL,
         MAGNITUDE TEXT,
         QUALIFIER TEXT
      )
   );

   SELECT
      *
   FROM
      read_parquet(
         @s3_read('who/who_life_expectancy.csv')
      );
   ```
   Note:
   - Indicate the kind of model you want to create (FULL, INCREMENTAL, etc.). Use incremental style for large datasets that will be run frequently.
   - Define the column schema for the new source.

3. **Add a transformation model**
   - Create a transformation model (if needed) in SQLMesh using Ibis. Add the model to the same folder as the source model above.

   - For Python-based transformation models, we recommend using the `generate_ibis_table` utility to reference other models/dependencies in the model you're developing:
   ```python
   # Import the table generation utility
   from macros.ibis_expressions import generate_ibis_table

   # Example transformation model
   @model(...)
   def entrypoint(evaluator: MacroEvaluator) -> str:
       # Generate the Ibis table expression
       model_1 = generate_ibis_table(
           evaluator,
           table_name="your_table",
           column_schema=get_sql_model_schema(...),
           schema_name="your_schema"
       )

       model_2 = generate_ibis_table(
           evaluator,
           table_name="your_table",
           column_schema=get_sql_model_schema(...),
           schema_name="your_schema"

       your_model = model_1.join(model_2, "your_join_key")

       return ibis.to_sql(your_model)
   ```
   - The sdg_indicators.py model is a good example of how to use the `generate_ibis_table` utility.
   - This utility simplifies working with SQLMesh and Ibis by handling table expression generation consistently. It helps prevent common integration issues and allows you to focus on your transformation logic.

4. **Run the Pipeline**
   ```bash
   # Process your new data
   docker build -t osaa-mvp .
   docker compose run --rm pipeline ingest
   docker compose run --rm pipeline etl
   ```

5. **Verify Results**
   - Check the S3 bucket for your processed data
   - Review any error messages if the process fails

### 4.4 Using SQLMesh UI to Verify Data
After running the pipeline, you can use the SQLMesh UI to verify the data.

1. **Start the UI**
   ```bash
   docker compose --profile ui up ui
   ```

2. **Access the Interface**
   - Open `http://localhost:8080` in your browser
   - Use the Editor tab to inspect individual models and their data
   - Use the Data Catalog to visualize data lineage and model's documentation

3. **Stop the UI**
   ```bash
   # Use Ctrl+C to stop when finished
   ```

### 4.5 Development vs Production

The pipeline has two main modes:
- **Development**: Your personal workspace for testing
- **Production**: Official data processing (restricted access)

Always work in development mode unless instructed otherwise:
```bash
# Run in development mode with your username
docker compose run --rm -e USERNAME=your_name pipeline etl
```

## 5. Getting Help

If you encounter issues or need assistance:
1. Check the troubleshooting section above
2. Review any error messages carefully
3. Contact your team lead or technical support

## 6. Project Structure

### 6.1 Repository Overview

The project repo consists of several key components:
1. The SQLMesh project containing all transformations
2. Docker container configuration files
3. Local development environment files

### 6.2 Directory Structure

```
osaa-mvp/
├── data/                      # Local representation of the datalake
│   ├── raw/                   # Source data files (CSV)
│   │   ├── edu/               # Contains educational datasets
│   │   └── wdi/               # World Development Indicators datasets
│   └── staging/               # Staging area for processed Parquet files
├── scratchpad/                # Temporary space for working code or notes
├── sqlMesh/                   # SQLMesh configuration and models
│   ├── models/                # SQLMesh model definitions
│   └── unosaa_data_pipeline.db            # DuckDB database for SQLMesh transformations
├── src/
│   └── pipeline/             # Core pipeline code
│       ├── ingest/           # Handles data ingestion from local raw csv to S3 parquet
│       ├── upload/           # Handles DuckDB transformed data upload to S3
│       ├── s3_sync/          # Handles SQLMesh database files sync with S3
│       ├── s3_promote/       # Handles data promotion between environments
│       ├── catalog.py        # Defines data catalog interactions
│       ├── config.py         # Stores configuration details
│       ├── utils.py          # Utility functions
├── .env_example              # Environment variables template
├── dockerfile                # Docker container definition
├── docker-compose.yml        # Docker services configuration
├── entrypoint.sh             # Docker container entry point script
├── justfile                  # Task automation for local execution
└── requirements.txt          # Python package dependencies
```

### 6.3 Cloud Storage Structure

```
s3://osaa-mvp/                 # Base bucket
│
├── dev/                     # Development environment
│   ├── landing/             # Landing zone for raw data
│   └── dev_{username}/      
|       └── staging/         # Development staging area
|           ├── _metadata/   # Metadata models
|           └── master/      # Final unified models
│
├── qa/                      # QA environment
│   ├── landing/             # QA landing zone
│   └── staging/             # QA staging area
│
└── prod/                    # Production environment
    ├── landing/             # Production landing zone
    └── staging/             # Production staging area
```

### 6.4 Source Code Structure

The `src/pipeline` directory contains the core pipeline commands:

```
src/pipeline/
├── ingest/                # Handles 'ingest' command
│   └── run.py             # Converts CSVs to Parquet
├── upload/                # Handles 'upload' command
│   └── run.py             # Uploads transformed data
├── s3_sync/               # Handles 's3_sync' command
│   └── run.py             # sync SQLMesh database files with S3
├── s3_promote/            # Handles 's3_promote' command
│   └── run.py             # Promotes data between environments
├── catalog.py             # Manages data locations
├── config.py              # Handles configuration
└── utils.py               # Shared utilities
```

## 7. CI/CD Workflows

### 7.1 Deploy to GHCR

[`.github/workflows/deploy_to_ghcr.yml`](.github/workflows/deploy_to_ghcr.yml)

Triggered when PRs are merged to main:
- Builds the container
- Runs QA process
- Pushes container to GitHub Container Registry

### 7.2 Run from GHCR

[`.github/workflows/run_from_ghcr.yml`](.github/workflows/run_from_ghcr.yml)

Triggered on every push:
- Builds the container
- Runs transform process
- Validates container execution

### 7.3 Daily Transform

[`.github/workflows/daily_transform.yml`](.github/workflows/daily_transform.yml)

Automated daily data processing:
- Runs at scheduled times
- Processes new data in production
- Updates analytics outputs

## 8. Security Notes

- Never commit `.env` files containing sensitive credentials
- Store all sensitive information as GitHub Secrets for CI/CD

## 9. Next Steps

### 9.1 Data Processing Improvements

- Add support for more data sources and formats
- Enhance data validation and quality checks
- Optimize transformation performance
- Expand the data catalog

### 9.2 User Interface

- Add web-based data exploration tools
- Create interactive dashboards
- Develop automated reporting capabilities
- Improve documentation and user guides

## Testing

The OSAA Data Pipeline includes a comprehensive testing framework to ensure data quality, reliability, and correctness.

### Test Structure

The testing framework is organized into three levels:

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── unit/                    # Fast, isolated unit tests
│   ├── test_ingest.py      # Ingestion module tests
│   ├── test_s3_sync.py     # S3 sync tests
│   ├── test_s3_promote.py  # Environment promotion tests
│   └── test_config.py      # Configuration tests
├── integration/             # Integration tests with mocked AWS
│   ├── test_s3_operations.py      # S3 workflow tests
│   └── test_pipeline_end_to_end.py # Complete pipeline tests
└── fixtures/                # Test data files

sqlMesh/tests/              # SQLMesh model tests
├── test_sdg_indicators.yaml
├── test_opri_indicators.yaml
└── test_wdi_indicators.yaml
```

### Running Tests

#### Run All Tests

```bash
# Run all tests with coverage
pytest

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=src/pipeline --cov-report=html
```

#### Run Specific Test Categories

```bash
# Run only unit tests (fast)
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only S3-related tests
pytest -m s3

# Run only SQLMesh tests
pytest -m sqlmesh

# Skip slow tests
pytest -m "not slow"
```

#### Run Specific Test Files

```bash
# Test ingestion module
pytest tests/unit/test_ingest.py

# Test S3 operations
pytest tests/integration/test_s3_operations.py

# Test specific function
pytest tests/unit/test_ingest.py::TestIngestInitialization::test_ingest_init_with_s3_enabled
```

#### Parallel Testing

```bash
# Run tests in parallel (faster)
pytest -n auto

# Run with 4 parallel workers
pytest -n 4
```

### Test Coverage

The project maintains a minimum test coverage of 70% for all pipeline code.

```bash
# Generate coverage report
pytest --cov=src/pipeline --cov-report=html

# View coverage in browser
open htmlcov/index.html
```

Current coverage targets:
- Unit tests: >70% code coverage
- Integration tests: All S3 operations covered
- SQLMesh tests: All data models tested

### Testing Best Practices

#### Unit Tests
- Test individual functions in isolation
- Mock external dependencies (S3, databases)
- Focus on edge cases and error handling
- Keep tests fast (<1 second each)

#### Integration Tests
- Use moto to mock AWS services
- Test complete workflows end-to-end
- Verify data integrity through pipelines
- Test error recovery scenarios

#### SQLMesh Tests
- Validate data transformations
- Check not_null constraints
- Verify unique grain requirements
- Test row count expectations
- Validate column renaming and joins

### Writing New Tests

#### Adding Unit Tests

```python
import pytest
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_my_function(duckdb_connection):
    """Test description."""
    # Arrange
    expected = "expected_result"

    # Act
    result = my_function()

    # Assert
    assert result == expected
```

#### Adding Integration Tests

```python
import pytest
from moto import mock_aws

@pytest.mark.integration
@pytest.mark.s3
def test_s3_workflow(s3_client, s3_bucket):
    """Test complete S3 workflow."""
    # Setup
    s3_client.put_object(Bucket=s3_bucket, Key="test.parquet", Body=b"data")

    # Execute
    result = perform_s3_operation(s3_bucket)

    # Verify
    assert result["success"] is True
```

#### Adding SQLMesh Tests

```yaml
test_model_not_null:
  model: sources.my_model
  description: Test that critical columns are not null
  inputs:
    my_schema.my_table:
      rows:
        - id: "001"
          value: 100
  outputs:
    query: |
      SELECT COUNT(*) as count
      FROM sources.my_model
      WHERE id IS NOT NULL
    rows:
      - count: 1
```

### Continuous Integration

Tests automatically run on:
- Every pull request
- Every push to main branch
- Scheduled nightly builds

CI Pipeline checks:
1. All tests pass
2. Coverage >70%
3. No linting errors
4. SQLMesh model tests pass

### Troubleshooting Tests

#### Common Issues

**Import Errors**
```bash
# Install test dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

**AWS Credential Errors**
```bash
# Set mock AWS credentials for tests
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
export AWS_DEFAULT_REGION=us-east-1
```

**DuckDB Extension Errors**
```bash
# Tests automatically install required extensions
# If issues persist, clear DuckDB cache
rm -rf ~/.duckdb/
```

**Coverage Not Generated**
```bash
# Ensure pytest-cov is installed
pip install pytest-cov

# Run with explicit coverage paths
pytest --cov=src/pipeline --cov-report=term-missing
```

### Test Data

Test fixtures provide realistic sample data for:
- SDG indicators (Sustainable Development Goals)
- OPRI indicators (Peace and Security)
- WDI indicators (World Bank Development Indicators)

Fixtures include:
- Sample CSV files with proper structure
- Mock S3 buckets and objects
- In-memory DuckDB databases
- Mock AWS credentials

### Performance Testing

```bash
# Show slowest tests
pytest --durations=10

# Profile test execution
pytest --profile

# Run only fast tests
pytest -m "not slow"
```

### Test Documentation

Each test includes:
- Clear descriptive name
- Docstring explaining purpose
- Arrange-Act-Assert structure
- Comments for complex logic
- Markers for categorization

Example:
```python
@pytest.mark.unit
@pytest.mark.s3
def test_s3_upload_validates_bucket_name(mock_s3_client):
    """Test that S3 upload validates bucket name before attempting upload.

    This test ensures that invalid bucket names are rejected early
    to prevent unnecessary API calls.
    """
    # Arrange: Setup invalid bucket name
    invalid_bucket = "invalid..bucket"

    # Act: Attempt upload
    with pytest.raises(ValueError, match="Invalid bucket name"):
        upload_to_s3(invalid_bucket, "key", b"data")

    # Assert: Verify no S3 API calls were made
    mock_s3_client.put_object.assert_not_called()
```

## Contact

- Mirian Lima (Project Sponsor) - mirian.lima@un.org
- Stephen Sciortino (Principal Engineer) - stephen.sciortino@un.org
- Project Link: [https://github.com/UN-OSAA/osaa-mvp.git](https://github.com/UN-OSAA/osaa-mvp.git)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgement

This project was **heavily inspired by** the work of [Cody Peterson](https://github.com/lostmygithubaccount), specifically the [ibis-analytics](https://github.com/ibis-project/ibis-analytics) repository. While the initial direction and structure of the project were derived from Cody's original work, significant modifications and expansions have been made to fit the needs and goals of this project.
