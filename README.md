# OSAA Data Pipeline MVP

## 1. Purpose

This project implements a **Minimum Viable Product** (MVP) Data Pipeline for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging modern data engineering tools to create an efficient and scalable data processing system.

## 2. Getting Started

This data pipeline operates locally on your computer while storing its results in the cloud (AWS). The process runs inside a Docker container but maintains all data outputs in AWS S3 cloud storage and uses AWS RDS PostgreSQL databases for state management. The pipeline uses DuckDB for efficient in-memory data processing and SQLMesh for managing SQL-based data transformations.

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
   - Update `.env` with the credentials provided by your project sponsor

3. **Execute the Pipeline**
   Standard execution with default settings:
   ```bash
   docker compose up
   ```

### Additional Runtime Options

1. **Execute Individual Components**
   ```bash
   # Run only the data ingestion
   docker compose run --rm pipeline ingest

   # Run only the transformation process
   docker compose run --rm pipeline transform

   # Run only the upload process
   docker compose run --rm pipeline upload
   ```

2. **Environment-Specific Execution**
   ```bash
   # Development environment with custom username
   docker compose run --rm -e TARGET=dev -e USERNAME=your_name pipeline etl

   # Quality Assurance environment
   docker compose run --rm -e TARGET=qa pipeline etl

   # Production environment
   docker compose run --rm -e TARGET=prod pipeline etl
   ```

### Environment Modes

The pipeline supports three operational modes:
- **Development** (`dev`): Individual workspace for testing and development
- **Quality Assurance** (`qa`): Verification environment for testing changes
- **Production** (`prod`): Production environment for official data processing

By default, the pipeline operates in development mode, providing isolated workspace for each user.

### Support Contact

For technical assistance or access requests, please contact:
- Mirian Lima (Project Sponsor) - mirian.lima@un.org
- Stephen Sciortino (Technical Lead) - stephen.sciortino@un.org

## 3. Project File Structure

### 3.1 Repository Overview
The project repo consists of several key components:
1. The SQLMesh project containing all transformations
2. Docker container configuration files
3. Local development environment files

### 3.2 Directory Structure
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
│   └── osaa_mvp.db            # DuckDB database for SQLMesh transformations
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

### 3.3 Cloud Storage Structure
```
s3://osaa-mvp/                 # Base bucket
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
│   ├── staging/                         # Staging area for processed Parquet |
│   │   ├── master
│   │   ├── reference
│   │   ├── source
│   │   └── product
│   │
│   └── analytics/                       # Transformed data
│
├── qa/                                 # QA environment (CICD)
│   ├── landing/
│   ├── staging/
│   └── analytics/
│
└── prod/                               # Production environment
│   ├── landing/
│   ├── staging/
│   └── analytics/
```

## 4. CI/CD Workflows

### 4.1 Deploy to GHCR
Triggered when PRs are merged to main:
- Builds the container
- Runs QA process
- Pushes container to GitHub Container Registry

### 4.2 Run from GHCR
Triggered on every push:
- Builds the container
- Runs transform process
- Validates container execution

## 5. Security Notes
- Never commit `.env` files containing sensitive credentials
- Store all sensitive information as GitHub Secrets for CI/CD

## 6. Next Steps
The next phase will focus on visualization layers:

### 6.1 Infrastructure Improvements
- Include a Motherduck destination
- Integrate Iceberg tables
- Add Hamilton orchestration
- Implement Open Lineage

### 6.2 Visualization Tools
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
