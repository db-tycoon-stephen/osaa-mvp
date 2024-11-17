import os

# Get the absolute path to the SQLMesh directory
SQLMESH_DIR = os.path.dirname(os.path.abspath(__file__))

# Use environment variable with fallback to local path
DB_PATH = os.getenv('DB_PATH', os.path.join(SQLMESH_DIR, 'osaa_mvp.db'))

# Print for debugging
print(f"SQLMesh using database path: {DB_PATH}")