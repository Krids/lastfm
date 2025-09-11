#!/bin/bash

# LastFM Session Analysis - Simple Docker Local Launcher

set -e

# Configuration
PIPELINE=${1:-complete}
IMAGE_NAME="lastfm-local"

echo "🎵 LastFM Session Analysis - Simple Docker Runner"
echo "================================================"

# Validate pipeline
case "$PIPELINE" in
    data-cleaning|session-analysis|complete|test)
        echo "🎯 Running pipeline: $PIPELINE"
        ;;
    *)
        echo "❌ Invalid pipeline: $PIPELINE"
        echo "Valid options: data-cleaning, session-analysis, complete, test"
        exit 1
        ;;
esac

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi

# Build image if needed
if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    echo "🔨 Building Docker image..."
    docker build -t "$IMAGE_NAME" -f Dockerfile.local .
fi

# Run the container
echo "🚀 Starting container..."
# Conditionally expose Spark UI port for non-test pipelines
if [ "$PIPELINE" != "test" ]; then
    PORT_FLAG="-p 4040:4040"
else
    PORT_FLAG=""
fi

docker run --rm \
    --name "lastfm-runner" \
    --memory="10g" \
    --cpus="8" \
    -v "$(pwd)/data:/app/data" \
    -v "$(pwd)/logs:/app/logs" \
    $PORT_FLAG \
    "$IMAGE_NAME" \
    "$PIPELINE"

echo "✅ Pipeline completed! Check ./data/output/ for results."
