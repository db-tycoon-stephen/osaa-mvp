# Automatically load environment variables from .env file
set dotenv-load

# Project-specific variables
package := "osaa-poc"
venv_dir := ".venv"
requirements_file := "requirements.txt"

# Include the src directory in PYTHONPATH
export PYTHONPATH := "src"

# Aliases for frequently used commands
alias fmt := format

# Display the list of recipes when no argument is passed
default:
    just --list

# Install runtime dependencies and set up virtual environment
install:
    @echo "Setting up {{venv_dir}} and dependencies..."
    @python -m venv {{venv_dir}}
    @. {{venv_dir}}/bin/activate
    @pip install --upgrade pip
    @pip install -r {{requirements_file}}
    @echo "Install complete!"

# Uninstall the package and clean up environment
uninstall:
    @echo "Uninstalling {{package}} and removing venv..."
    @pip uninstall -y {{package}}
    @rm -rf {{venv_dir}}
    @echo "Uninstall complete!"

# Format the codebase using ruff
format:
    @echo "Formatting the codebase using ruff..."
    @ruff format .

# Run Ingest pipeline with optional arguments for sources
ingest:
    @echo "Running the Ingest process..."
    @python -m pipeline.ingest.run

# Run SQLMesh transformations
transform:
    echo "Running SQLMesh transformations..."
    cd sqlMesh && sqlmesh plan --auto-apply

# Run Upload pipeline with optional arguments for sources
upload:
    @echo "Running the Upload process"
    @python -m pipeline.upload.run

# Run the complete pipeline
etl: ingest transform upload
    @echo "Pipeline complete!"

# Open the project repository in the browser
repo:
    @echo "Opening the project repository in the browser..."
    @open https://github.com/UN-OSAA/osaa-mvp.git
