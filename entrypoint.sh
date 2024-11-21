#!/bin/bash
set -e  # Exit on error

case "$1" in
  "ingest")
    python -m pipeline.ingest.run
    ;;
  "transform")
    cd sqlMesh
    sqlmesh plan --auto-apply --include-unmodified --create-from prod --no-prompts "${TARGET:-dev}"
    ;;
  "transform_dry_run")
    export ENABLE_S3_UPLOAD=false
    export RAW_DATA_DIR=/app/data/raw
    
    echo "Start local ingestion"
    python -m pipeline.ingest.run
    echo "End ingestion"
    
    echo "Start sqlMesh"
    cd sqlMesh
    sqlmesh plan --auto-apply --include-unmodified --create-from prod --no-prompts "${TARGET:-dev}"
    echo "End sqlMesh"
    ;;
  "upload")
    python -m pipeline.upload.run
    ;;
  "etl")
    echo "Starting pipeline"
    
    echo "Start ingestion"
    python -m pipeline.ingest.run
    echo "End ingestion"
    
    echo "Start sqlMesh"
    cd sqlMesh
    sqlmesh plan --auto-apply --include-unmodified --create-from prod --no-prompts "${TARGET:-dev}"
    echo "End sqlMesh"
    
    cd ..
    echo "Start upload"
    python -m pipeline.upload.run
    echo "End upload"
    ;;
  *)
    echo "Usage: docker run <image> [ingest|transform|upload|etl]"
    exit 1
    ;;
esac