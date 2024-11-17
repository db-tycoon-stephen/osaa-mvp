# OSAA Data Pipeline MVP

## Overview

This project implements a **Data Pipeline Minimum Viable Product** (MVP) for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging Ibis, DuckDB, the Parquet format and S3 to create an efficient and scalable data processing system. The pipeline ingests data from various sources, transforms it, and stores it in a data lake structure, enabling easy access and analysis.

## Project Structure

```
osaa-mvp/
├── datalake/                  # Local representation of the datalake
│   ├── raw/                   # Source data files (CSV)
│   │   ├── edu/               # Contains educational datasets
│   │   ├── wdi/               # World Development Indicators datasets
│   └── staging/               # Staging area for processed Parquet files
├── scratchpad/                # Temporary space for working code or notes
├── src/
│   └── pipeline/              # Core pipeline code
│       ├── etl/               # Extract, Transform, Load scripts
│       │   ├── sources/       # Source-specific data processing (e.g., WDI, EDU)
│       ├── ingest/            # Handles data ingestion from local raw csv to S3 parquet
│       ├── catalog.py         # Defines data catalog interactions
│       ├── config.py          # Stores configuration details (e.g., paths, S3 settings)
│       ├── utils.py           # Utility functions
├── .env                       # Environment variables configuration
├── justfile                   # Automates common tasks (installation, running pipelines)
├── pyproject.toml             # Project metadata and dependencies
├── requirements.txt           # Python package dependencies
```

## Key Components

- **Ibis**: A Python framework for data analysis, used to write expressive and portable data transformations. It provides a high-level abstraction over SQL databases like DuckDB, allowing for cleaner, more Pythonic data manipulation.
- **DuckDB**: A highly performant in-memory SQL engine for analytical queries, used for efficient data processing and querying, in order to process, convert, and interact with Parquet files and S3.
- **Parquet**: A columnar storage file format, used for efficient data storage and retrieval. Used as the core format for storing processed data.
- **S3**: Amazon Simple Storage Service, used as the cloud storage solution for the data lake, storing both raw (landing folder) and processed (staging folder) data.

## How It Works

### Ingestion Process
The Ingest Pipeline reads raw CSV data from `datalake/raw/<source>`, processes it, converts it into Parquet format using DuckDB, and uploads the results to an S3 bucket under the `landing/<source>` folder.

### ETL Process
The ETL Pipeline extracts data, transforms it (cleaning, filtering, joining), and outputs it into a master Parquet file and a DuckDB file, stored locally in `datalake/staging/master/` and optionally uploaded to `staging/master` folder in S3.

## Getting Started

### Prerequisites
- Python versions 3.9 to 3.11 (3.12 not supported)
- AWS account with S3 access
- Just (command runner) - for running predefined commands

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
   
   # Edit .env with your AWS credentials
   # Required variables:
   AWS_ACCESS_KEY_ID=<your-aws-access-key>
   AWS_SECRET_ACCESS_KEY=<your-aws-secret-key>
   AWS_DEFAULT_REGION=<your-aws-region>
   S3_BUCKET_NAME=osaa-poc
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

Use the `justfile` to run common tasks:

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