# OSAA Data Pipeline MVP

## Overview

This project implements a **Data Pipeline Minimum Viable Product** (MVP) for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging Ibis, DuckDB, the Parquet format and S3 to create an efficient and scalable data processing system. The pipeline ingests data from various sources, transforms it, and stores it in a data lake structure, enabling easy access and analysis.

## Project Structure
```
osaa-mvp/
├── datalake/                  # Local representation of the datalake
│   ├── raw/                   # Source data files (CSV)
│   │   ├── edu/               # Contains educational datasets
│   │   └── wdi/               # World Development Indicators datasets
│   └── staging/               # Staging area for processed Parquet files
├── scratchpad/                # Temporary space for working code or notes
├── sqlMesh/                   # SQLMesh configuration and models
│   ├── models/                # SQLMesh model definitions
│   └── osaa_mvp.db           # DuckDB database for SQLMesh transformations
├── src/
│   └── pipeline/             # Core pipeline code
│       ├── ingest/           # Handles data ingestion from local raw csv to S3 parquet
│       ├── upload/           # Handles DuckDB transformed data upload to S3
│       ├── catalog.py        # Defines data catalog interactions
│       ├── config.py         # Stores configuration details (e.g., paths, S3 settings)
│       ├── utils.py          # Utility functions
├── .env                      # Environment variables configuration
├── dockerfile                # Docker container definition
├── docker-compose.yml        # Docker services and environment setup
├── entrypoint.sh             # Docker container entry point script
├── justfile                  # Automates common tasks (installation, running pipelines) for local execution w/o Docker
└── requirements.txt          # Python package dependencies
```
## System Architecture
The system architecture diagram can be found in [system_architecture.md](system_architecture.md)

## Key Components

- **Ibis**: A Python framework for data analysis, used to write expressive and portable data transformations. It provides a high-level abstraction over SQL databases like DuckDB, allowing for cleaner, more Pythonic data manipulation.
- **DuckDB**: A highly performant in-memory SQL engine for analytical queries, used for efficient data processing and querying, in order to process, convert, and interact with Parquet files and S3.
- **Parquet**: A columnar storage file format, used for efficient data storage and retrieval. Used as the core format for storing processed data.
- **SQLMesh**: A SQL-based data management tool, used to manage the SQLMesh models and transformations.
- **S3**: Amazon Simple Storage Service, used as the cloud storage solution for the data lake, storing both raw (landing folder) and processed (staging folder) data.

## How It Works
The data pipeline consists of three main stages:

### Ingestion Process (`ingest/run.py`)
- Reads raw CSV files from `datalake/raw/<source>` directories
- Converts them to Parquet format using DuckDB
- Uploads the Parquet files to S3 under `<env>/landing/<source>/` folders
- Creates separate folders for different data sources (edu, wdi)

### Transformation Process (SQLMesh)
- Reads Parquet files from the S3 landing zone
- Performs transformations using SQLMesh models:
- Stores transformed data in local DuckDB database (`osaa_mvp.db`)
- Outputs transformed data to S3 under `<env>/transformed/<schema>/` folders

### Upload Process (`upload/run.py`)
- Takes transformed data from the DuckDB database
- Uploads final transformed datasets to S3 under `<env>/transformed/` directory
- Currently focuses only on uploading WDI (World Development Indicators) transformed data

The environment (`<env>`) in S3 paths is determined by configuration:
   - Production: `prod/`
   - Integration: `int/`
   - Development: `dev_<username>/`

## Getting Started

### Prerequisites
- Python versions 3.9 to 3.11 (3.12 not supported)
- AWS account with S3 access
- Just (command runner) - for running predefined commands
- Docker Desktop (optional) - for running the pipeline in a containerized environment

### Setup
1. Clone the repository:
   ```
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp
   ```

2. Check if `just` is installed:
   ```
   just --version
   ```
   If `just` is not installed, follow the instructions below to install it:

   - On macOS, you can use Homebrew:
     ```
     brew install just
     ```

   - On Linux, you can use the package manager for your distribution. For example, on Ubuntu:
     ```
     sudo apt install just
     ```

   - On Windows, you can use Scoop:
     ```
     scoop install just
     ```

3. Install dependencies using `just`:
   ```
   just install
   ```

4. Set up your environment variables:
   ```bash
   # Copy the example environment file
   cp .env.example .env
   ```
   Edit .env with your AWS credentials
   
   Required variables:
   ```bash
   # AWS Credentials
   AWS_ACCESS_KEY_ID=<your-aws-access-key>
   AWS_SECRET_ACCESS_KEY=<your-aws-secret-key>
   AWS_DEFAULT_REGION=<your-aws-region>

   # S3 Configuration
   S3_BUCKET_NAME=osaa-mvp
   TARGET=dev
   USERNAME=<your-name>
   ```

   These credentials are used for:
   - S3 access for data storage
   - DuckDB S3 integration
   - Local development and Docker execution

## Raw Data Setup

The raw data for this project is too large to be directly included in the GitHub repository. Instead, it's compressed and stored using Git Large File Storage (LFS). Follow these steps to set up the raw data correctly:

1. Install Git LFS:

   - **macOS**: Use Homebrew to install Git LFS:
     ```
     brew install git-lfs
     ```

   - **Linux**: Use your distribution's package manager. For example, on Ubuntu:
     ```
     sudo apt install git-lfs
     ```

   - **Windows**: Use the Git for Windows installer or a package manager like Scoop:
     ```
     scoop install git-lfs
     ```

2. Initialize Git LFS in your repository:
   ```
   git lfs install
   ```

3. Clone the repository (if you haven't already):
   ```
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp
   ```

3. Pull the LFS files:
   ```
   git lfs pull
   ```

4. Locate the compressed raw data file:
    The compressed file is in the root directory of the project, named `datalake.zip`.

5. Decompress the raw data:
   ```
   unzip `datalake.zip`
   ```

6. Verify the data structure:
   After decompression, you should see the following structure in your `datalake/` directory:
   ```
   datalake/
   ├── raw/
   │   ├── edu/
   │   │   ├── OPRI_DATA_NATIONAL.csv
   │   │   ├── OPRI_LABEL.csv
   │   │   ├── SDG_DATA_NATIONAL.csv
   │   │   ├── SDG_LABEL.csv
   │   ├── wdi/
   │   │   ├── WDICSV.csv
   │   │   ├── WDISeries.csv
   ```
   
Now your raw data is set up correctly, and you can proceed with running the pipeline as described below.

### Running the Pipeline

#### Using the `justfile` to run common tasks:

```bash
just ingest    # Run the ingestion process
just transform # Run the SQLMesh transformations
just upload    # Run the upload process
just etl       # Run the complete pipeline (ingest → transform → upload)
```

You can see all available commands by running:
```bash
just --list
```

#### Running with Docker
1. Build the Docker image:
   ```
   docker build -t osaa-mvp .
   ```

### Environment Configuration
The pipeline supports different execution environments controlled through environment variables.
The main variables that control behavior are:
- TARGET: Controls both S3 paths and SQLMesh environments (`dev`, `int`, `prod`). Default is `dev`
- USERNAME: Used for S3 paths in `dev` environment. Default is `default`

####  Standard Execution
Run the complete pipeline with default settings:
```bash
docker compose up
```
This uses the environment variables from your `.env` file.

#### One-off Pipeline Components
You can run specific parts of the pipeline:
1. Run only the ingestion process:
```bash
docker compose run --rm pipeline ingest
```

2. Run only the upload process:
```bash
docker compose run --rm pipeline upload
```


#### Environment Variable Control at Runtime
You can override environment variables when running specific commands without modifying your `.env` file. This is useful for:
- Testing different environments
- Running as different users
- Temporary configuration changes

#### Examples
1. Run as a specific user in development:
Uses `username` for S3 paths:
```bash
docker compose run --rm -e TARGET=dev -e USERNAME=johndoe pipeline etl
```

This creates S3 paths like: `s3://osaa-poc/dev_johndoe/landing/`

2. Run as integration environment:
Uses `int` for S3 paths:
```bash
docker compose run --rm -e TARGET=int pipeline etl
```

This creates S3 paths like: `s3://osaa-poc/int/landing/`

3. Run as production environment:
Uses `prod` for S3 paths:
```bash
docker compose run --rm -e TARGET=prod pipeline etl
```

This creates S3 paths like: `s3://osaa-poc/prod/landing/`

#### Testing Configuration
To verify your environment settings before running the pipeline:
```bash
docker compose run --rm pipeline config_test
```
This will output all configured paths and S3 locations based on your environment settings.


#### S3 Folder Structure

```
s3://osaa-mvp/                           # Base bucket
│
├── dev_{username}/                      # For dev environment (e.g., johndoe/)
│   ├── landing/                         # Raw data landing zone
│   │   ├── edu/                         # Education data
│   │   │   ├── SDG_LABEL.parquet
│   │   │   ├── OPRI_DATA_NATIONAL.parquet
│   │   │   ├── SDG_DATA_NATIONAL.parquet
│   │   │   └── OPRI_LABEL.parquet
│   │   └── wdi/                         # World Development Indicators
│   │       ├── WDICSV.parquet
│   │       └── WDISeries.parquet
│   │
│   ├── transformed/                     # Transformed data
│   │   └── wdi/
│   │       └── wdi_transformed.parquet
│
├── int/                                 # Integration environment (CICD)
│   ├── landing/
│   ├── transformed/
│   └── staging/
│
└── prod/                                # Production environment
    ├── landing/
    ├── transformed/
    └── staging/
```



## Next Steps

The next phase of this project will focus on experimenting with different visualization layers to effectively present the processed data. This may include:

- Include a Motherduck destination in the etl pipeline
- Integrate the use of:
    - Iceberg tables for better cataloguing
    - Hamilton for orchestration
    - Open Lineage for data lineage
- Integration with BI tools like Tableau or Power BI
- Experimentation with code-based data app/report/dashboard development using Quarto, Evidence, Marimo and Observable Framework.
- Exploration of data science notebooks for advanced analytics, like Marimo, Quarto, Hex and Deepnote.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

Mirian Lima (Project Sponsor) - mirian.lima@un.org
Stephen Sciortino (Principal Engineer, Consultant) - stephen.sciortino@un.org; stephen@databasetycoon.com
Project Link: [https://github.com/UN-OSAA/osaa-mvp.git](https://github.com/UN-OSAA/osaa-mvp.git)


## Acknowledgement

This project was **heavily inspired by** the work of [Cody Peterson](https://github.com/lostmygithubaccount), specifically the [ibis-analytics](https://github.com/ibis-project/ibis-analytics) repository. While the initial direction and structure of the project were derived from Cody’s original work, significant modifications and expansions have been made to fit the needs and goals of this project, resulting in a codebase that diverges substantially from the original implementation.