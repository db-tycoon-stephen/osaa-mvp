#!/bin/bash
case "$1" in
  "ingest")
    python -m pipeline.ingest.run
    ;;
  "transform")
    cd sqlMesh && sqlmesh plan --auto-apply
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
    cd sqlMesh && sqlmesh run
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