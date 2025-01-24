# OSAA Data Pipeline MVP

## 1. Purpose

This project implements a **Minimum Viable Product** (MVP) Data Pipeline for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging modern data engineering tools to create an efficient and scalable data processing system.

## 2. Getting Started

This data pipeline operates locally on your computer while storing its results in the cloud (AWS). The process runs inside a Docker container but maintains all data outputs in AWS S3 cloud storage and uses AWS RDS PostgreSQL databases for state management. The pipeline processes data using two specialized tools: DuckDB for efficient data operations and SQLMesh for managing transformation commands.

This ReadMe will show you how to:
- Download the project code and run it locally with Docker
- Run the pipeline in different modes and environments
- Access the data lake and data warehouse

### 2.1 Key Technologies
- **[Docker](https://www.docker.com/)**: Containerization platform that ensures consistent execution across different computers
- **[SQLMesh](https://sqlmesh.com/)**: Data management tool for SQL-based data transformations
- **[Ibis](https://ibis-project.org/)**: Python-based transformation framework, to be used for SQLMesh models
- **[DuckDB](https://duckdb.org/)**: In-memory analytical database for fast in-memory data processing
- **[Parquet](https://parquet.apache.org/)**: Columnar storage format for efficient data storage
- **[AWS S3](https://aws.amazon.com/s3/)**: Cloud storage for data lake architecture, hosted on AWS
- **[GitHub](https://github.com/)**: Version control and code collaboration platform
- **[GitHub Actions](https://github.com/features/actions)**: Automated workflow and CI/CD platform integrated with GitHub

### 2.2 Prerequisites

#### A. Required Software
- [Docker Desktop](https://www.docker.com/products/docker-desktop/): Application containerization platform that packages the pipeline and all its dependencies into a single, runnable unit. Available for Windows, Mac, and Linux.
- [Git](https://git-scm.com/downloads): Version control system that lets you download and track changes to the project code. Choose the version for your operating system (Windows, Mac, or Linux).

**Setting up Git:**
> *Windows:*
> 1. Download Git from [git-scm.com](https://git-scm.com/downloads)
> 2. Run the installer (Git-X.XX.X-64-bit.exe)
> 3. Open Command Prompt or PowerShell and verify installation:
>    ```bash
>    git --version
>    ```

*Mac:*
1. Open Terminal
2. Install using Homebrew (recommended):
   ```bash
   brew install git
   ```
3. Verify installation:
   ```bash
   git --version
   ```

*Linux (Ubuntu/Debian):*
1. Open Terminal
2. Update package list and install:
   ```bash
   sudo apt update
   sudo apt install git
   ```
3. Verify installation:
   ```bash
   git --version
   ```

#### B. Required Cloud Access
Your project sponsor will provide access credentials for:
- AWS S3: Cloud storage for processed data
- AWS RDS: Cloud database for pipeline state management

Note: After installing Docker Desktop, you'll need to start the application before running any pipeline commands. Git installation will provide the `git` command in your terminal/command prompt.

### 2.3 Basic Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp
   ```

2. **Configure Access Credentials**
   - Copy the example configuration file:
     ```bash
     cp .env.example .env
     ```
   - Update `.env` with the credentials provided by your project sponsor. Here's what each setting controls:

   **AWS Configuration:**
   ```bash
   # Access credentials for AWS services
   AWS_ROLE_ARN=arn:aws:iam::<account-id>:role/<role-name>  # IAM role to assume
   AWS_DEFAULT_REGION=us-east-1                             # AWS region where resources are located

   # Optional: Enable/disable S3 upload functionality
   ENABLE_S3_UPLOAD=true                                    # Set to false to disable S3 uploads

   # S3 bucket settings
   S3_BUCKET_NAME=unosaa-data-pipeline                      # Where the data pipeline stores its files
   ```

   The pipeline uses AWS Security Token Service (STS) to assume the specified role, which is more secure than using long-term access keys. The system will:
   1. Validate the AWS role ARN and region
   2. Attempt to assume the specified role using STS
   3. Create temporary credentials for secure S3 access
   4. Validate the credentials by testing S3 bucket access

   If any validation step fails, the pipeline will provide detailed error messages and troubleshooting steps.

   **Required IAM Role Permissions:**
   The IAM role specified in `AWS_ROLE_ARN` needs the following permissions:
   ```json
   {
      "Version": "2012-10-17",
      "Statement": [
         {
               "Effect": "Allow",
               "Action": [
                  "sts:AssumeRole"
               ],
               "Resource": insert_arn_here
         },
         {
               "Effect": "Allow",
               "Action": [
                  "s3:*"
               ],
               "Resource": "arn:aws:s3:::unosaa-data-pipeline"
         }
      ]
   }
   ```

   This policy grants the necessary permissions for the pipeline to:
   - List bucket contents
   - Read existing objects
   - Upload new objects

   **Environment-Specific S3 Paths:**
   The pipeline automatically organizes data in S3 based on your environment:
   - Production: `s3://unosaa-data-pipeline/prod/...`
   - Development: `s3://unosaa-data-pipeline/dev/{TARGET}_{USERNAME}/...`

   Each environment has two main folders:
   - `/landing`: For raw data uploads
   - `/staging`: For processed data

   **Pipeline Control:**
   ```bash
   # Controls which environment the pipeline runs in
   TARGET=dev                          # Options: dev (testing), qa (verification), prod (production)
   USERNAME=your_name                  # Used to create personal workspace in dev environment
   ```

   **Database Connection:**
   ```bash
   # Pipeline state management
   GATEWAY=shared_state              # Controls where transformation state is stored
                                    # Options: shared_state (PostgreSQL), local (DuckDB)
   ```

   These credentials enable:
   - Storing processed data in AWS S3
   - Tracking data transformations in a shared database
   - Creating isolated workspaces for development
   - Managing different deployment environments

3. **Build and Execute the Pipeline**
   First, build the Docker container to include any code changes:
   ```bash
   docker build -t osaa-mvp .
   ```

   Then run the complete pipeline:
   ```bash
   docker compose run --rm pipeline etl
   ```

   Note: Always rebuild the container when you make changes to the code


## 3. Running the Pipeline

There are two primary ways to use this project:
1. Running existing transformations in your development environment
2. Adding new datasets and transformations to the project

### 3.1 Running Existing Transformations

#### Basic Execution
For most users, you'll want to run the complete pipeline:
```bash
docker compose run --rm pipeline etl
```
This will process all datasets through the entire pipeline (ingest → transform → upload).

#### Specific Commands
You can also run individual parts of the pipeline:
```bash
# Run only data ingestion (CSV → Parquet)
docker compose run --rm pipeline ingest

# Run only transformations
docker compose run --rm pipeline transform

# Run only the final upload
docker compose run --rm pipeline upload
```


### 3.2 Adding New Datasets

To add a new dataset to the project:

1. **Add Source Data**
   ```bash
   # Add your CSV file to the appropriate source directory
   data/raw/<source_name>/your_data.csv
   ```

2. **Create SQLMesh Models**
   All datasets must be transformed into a vertical format and unioned into the final `models/master/indicators.py` model. The required format is:
   ```
   country_id    indicator_id    year    value    indicator_label    database
   ----------    ------------    ----    -----    ---------------    --------
   AGO           EDU_001        2020    42.5     "Education..."     "edu"
   AGO           EDU_002        2020    78.3     "Primary..."       "edu"
   ```

   Create your models in this structure:
   ```
   sqlMesh/models/
   ├── sources/                # Source data models and transformations
   │   └── your_source/       # One folder per data source
   │       ├── data.sql       # Raw data ingestion
   │       └── transform.py   # Data transformations
   └── master/                # Final unified models
       └── indicators.py      # Combined dataset
   ```

   All datasets must be transformed into a vertical format and unioned into the final `models/master/indicators.py` model.

3. **Test Your Changes**
   ```bash
   # Run just your new transformation
   docker compose run --rm pipeline transform
   ```

   Verify your data appears correctly in the final indicators model.

### 3.3 Additional Runtime Options
The pipeline commands are defined in our `justfile` and exposed through Docker Compose. Here are the available commands and their purposes:

1. **Core Pipeline Commands**
   ```bash
   # Run the complete pipeline (equivalent to: ingest → transform → upload)
   docker compose run --rm pipeline etl

   # Run only the data ingestion (CSV → Parquet, then upload to S3)
   docker compose run --rm pipeline ingest

   # Run only the transformation process (SQLMesh models)
   docker compose run --rm pipeline transform

   # Run only the upload process (transformed data → S3)
   docker compose run --rm pipeline upload
   ```

2. **Development and Testing Commands**
   ```bash
   # Run etl
   docker compose run --rm pipeline etl
   ```

3. **Environment Control**
   The pipeline defaults to development mode (`dev`), which is the recommended environment for most users. Only change this setting in specific situations:
   ```bash
   # Default development environment (recommended for most users)
   docker compose run --rm pipeline etl
   # or explicitly:
   docker compose run --rm -e TARGET=dev -e USERNAME=your_name pipeline etl

   # Quality Assurance environment (used by CI/CD pipelines)
   # Only use when testing changes for production
   docker compose run --rm -e TARGET=qa -e GATEWAY=shared_state pipeline etl

   # Production environment (restricted access)
   # Only use when authorized to process official data
   docker compose run --rm -e TARGET=prod -e GATEWAY=shared_state pipeline etl
   ```

   Note: When setting environment variables with docker compose run, the -e flags must come BEFORE the service name (pipeline).
   ```bash
   # CORRECT:
   docker compose run --rm -e TARGET=prod pipeline etl

   # INCORRECT - variables won't be set:
   docker compose run --rm pipeline etl -e TARGET=prod
   ```

   Stay in the development environment unless specifically instructed otherwise by the project team. This ensures data safety and provides an isolated workspace for your work.

4. **Data Flow Examples**
   ```bash
   # Ingest new source data and transform it
   docker compose run --rm pipeline ingest transform

   # Transform and upload without new ingestion
   docker compose run --rm pipeline transform upload
   ```

These commands come from:
- `justfile`: Defines the core commands (`etl`, `ingest`, `transform`, etc.)
- `docker-compose.yml`: Exposes the commands through the `pipeline` service
- `entrypoint.sh`: Handles command execution inside the container

The pipeline is modular - you can run any combination of steps in sequence by listing them as arguments to the pipeline service.

### 3.3 Environment Modes
The pipeline supports three operational modes:
- **Development** (`dev`): Individual workspace for testing and development
- **Quality Assurance** (`qa`): Verification environment for testing changes
- **Production** (`prod`): Production environment for official data processing

By default, the pipeline operates in development mode, providing isolated workspace for each user.

## 4. Project File Structure

### 4.1 Repository Overview
The project repo consists of several key components:
1. The SQLMesh project containing all transformations
2. Docker container configuration files
3. Local development environment files

### 4.2 Directory Structure
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
│       ├── catalog.py        # Defines data catalog interactions
│       ├── config.py         # Stores configuration details (e.g., paths, S3 settings)
│       ├── utils.py          # Utility functions
├── .env_example              # Environment variables configuration (template file)
├── dockerfile                # Docker container definition
├── docker-compose.yml        # Docker services and environment setup
├── entrypoint.sh             # Docker container entry point script
├── justfile                  # Automates common tasks (installation, running pipelines) for local execution w/o Docker
└── requirements.txt          # Python package dependencies
```

### 4.3 Cloud Storage Structure
```
s3://osaa-mvp/                 # Base bucket
│
├── dev_{username}/           # Development environment (e.g., dev_johndoe/)
│   ├── landing/             # Landing zone for raw data
│   └── staging/             # Staging area
│       ├── source/          # Source data models
│       └── master/          # Final unified models
│
├── qa/                      # QA environment
│   ├── landing/            # QA landing zone
│   └── staging/            # QA staging area
│       ├── source/         # Source data models
│       └── master/         # Final unified models
│
└── prod/                    # Production environment
    ├── landing/            # Production landing zone
    └── staging/            # Production staging area
        ├── source/         # Source data models
        └── master/         # Final unified models
```

Each environment (dev, qa, prod) has its own landing zone under `<environment>/landing/`. For example:
- Development: `s3://osaa-mvp/dev_username/landing/`
- QA: `s3://osaa-mvp/qa/landing/`
- Production: `s3://osaa-mvp/prod/landing/`

### 4.4 Source Code Structure

The `src/pipeline` directory contains the Python code that powers the core pipeline commands. Here's how the code maps to the commands:

```
src/pipeline/
├── ingest/                 # Handles 'ingest' command
│   └── run.py             # Converts CSVs to Parquet and uploads to S3
├── upload/                 # Handles 'upload' command
│   └── run.py             # Uploads transformed data to S3
├── catalog.py             # Manages data locations and paths
├── config.py              # Handles environment variables and settings
└── utils.py               # Shared utility functions
```

**Mapping Commands to Processes:**
- `docker compose run --rm pipeline ingest`
  - Runs the code in `ingest/run.py`
  - Reads CSV files from `data/raw/`
  - Converts them to Parquet format
  - Uploads to the S3 landing zone

- `docker compose run --rm pipeline transform`
  - Uses SQLMesh models in `sqlMesh/models/`
  - Reads data from S3 landing zone
  - Applies transformations
  - Stores results in DuckDB

- `docker compose run --rm pipeline upload`
  - Runs the code in `upload/run.py`
  - Takes transformed data from DuckDB
  - Uploads to S3 analytics zone

- `docker compose run --rm pipeline etl`
  - Runs all three commands in sequence: ingest → transform → upload

## 5. CI/CD Workflows

### 5.1 Deploy to GHCR
Triggered when PRs are merged to main:
- Builds the container
- Runs QA process
- Pushes container to GitHub Container Registry

### 5.2 Run from GHCR
Triggered on every push:
- Builds the container
- Runs transform process
- Validates container execution

### 5.3 Daily Transform
Automated daily data processing:
- Runs at scheduled times
- Processes new data in production
- Updates analytics outputs

## 6. Security Notes
- Never commit `.env` files containing sensitive credentials
- Store all sensitive information as GitHub Secrets for CI/CD

## 7. Next Steps
The next phase will focus on visualization layers:

### 7.1 Infrastructure Improvements
- Include a Motherduck destination
- Integrate Iceberg tables
- Add Hamilton orchestration
- Implement Open Lineage

### 7.2 Visualization Tools
- BI tool integration (Tableau, Power BI)
- Code-based dashboards and reports:
  - Quarto
  - Evidence
  - Marimo
  - Observable Framework
- Data science notebooks:
  - Marimo
  - Quarto
  - Hex
  - Deepnote

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

Mirian Lima (Project Sponsor) - mirian.lima@un.org
Stephen Sciortino (Principal Engineer, Consultant) - stephen.sciortino@un.org; stephen@databasetycoon.com
Project Link: [https://github.com/UN-OSAA/osaa-mvp.git](https://github.com/UN-OSAA/osaa-mvp.git)


## Acknowledgement

This project was **heavily inspired by** the work of [Cody Peterson](https://github.com/lostmygithubaccount), specifically the [ibis-analytics](https://github.com/ibis-project/ibis-analytics) repository. While the initial direction and structure of the project were derived from Cody’s original work, significant modifications and expansions have been made to fit the needs and goals of this project, resulting in a codebase that diverges substantially from the original implementation.
