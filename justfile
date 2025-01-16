# Automatically load environment variables from .env file
set dotenv-load

# Project-specific variables
package := "osaa-mvp"
venv_dir := ".venv"
requirements_file := "requirements.txt"
target := env_var_or_default("TARGET", "dev")
gateway := env_var_or_default("GATEWAY", "local")

# Include the src directory in PYTHONPATH
export PYTHONPATH := "src"

# Display the list of recipes when no argument is passed
default:
    just --list

# Install runtime dependencies and set up virtual environment
install:
    @echo "🚀 OSAA MVP: Setting up development environment..."
    @echo "   Creating virtual environment in {{venv_dir}}..."
    @uv venv {{venv_dir}}
    @echo "   Upgrading pip..."
    @uv pip install --upgrade pip
    @echo "   Installing project dependencies..."
    @uv pip install -r {{requirements_file}}
    @echo "✅ Development environment setup complete!"

# Uninstall the package and clean up environment
uninstall:
    @echo "🧹 OSAA MVP: Cleaning up development environment..."
    @rm -rf {{venv_dir}}
    @echo "✨ Environment cleaned successfully!"

# Run Ingest pipeline with optional arguments for sources
ingest:
    @echo "📥 OSAA MVP: Starting data ingestion process..."
    @uv run python -m pipeline.ingest.run
    @echo "✅ Data ingestion completed successfully!"

# Run SQLMesh transformations
transform:
    @echo "🔄 OSAA MVP: Running SQLMesh transformations..."
    @cd sqlMesh && uv run sqlmesh --gateway {{gateway}} plan --auto-apply --include-unmodified --create-from prod --no-prompts {{target}}
    @echo "✅ SQLMesh transformations completed!"

# Run SQLMesh transformations in dry-run mode (no S3 uploads)
transform_dry_run:
    @echo "🧪 OSAA MVP: Running pipeline in dry-run mode..."
    @export ENABLE_S3_UPLOAD=false
    @export RAW_DATA_DIR=data/raw
    @echo "   Performing local data ingestion..."
    @uv run python -m pipeline.ingest.run
    @echo "   Local ingestion complete. Starting dry-run transformations..."
    @cd sqlMesh && uv run sqlmesh --gateway {{gateway}} plan --auto-apply --include-unmodified --create-from prod --no-prompts {{target}}
    @echo "✅ Dry-run pipeline completed successfully!"

# Run the complete pipeline
etl: ingest transform
    @echo "🚀 OSAA MVP: Full ETL pipeline executed successfully!"

# Clean up development artifacts
clean:
    @echo "🧹 Cleaning up development artifacts..."
    @rm -rf .venv
    @find . -type f -name "*.pyc" -delete
    @rm -rf .mypy_cache .pytest_cache htmlcov

# Open the project repository in the browser
repo:
    @echo "🌐 OSAA MVP: Opening project repository..."
    @open https://github.com/UN-OSAA/osaa-mvp.git
