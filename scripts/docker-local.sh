#!/bin/bash

# LastFM Session Analysis - Simple Docker Local Launcher

set -e

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

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

# Function to check if port is available
check_port_available() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 1  # Port is in use
    else
        return 0  # Port is available
    fi
}

# Function to find next available port
find_available_port() {
    local start_port=$1
    local port=$start_port
    while [ $port -le $((start_port + 10)) ]; do
        if check_port_available $port; then
            echo $port
            return
        fi
        port=$((port + 1))
    done
    echo ""  # No port found
}

# Build image if needed
if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    echo "🔨 Building Docker image..."
    docker build -t "$IMAGE_NAME" -f Dockerfile.local .
fi

echo "🚀 Starting container..."

# Clean up any existing containers with the same name
echo "🧹 Cleaning up any existing containers..."
docker rm -f "lastfm-runner" 2>/dev/null || true

# Environment setup
COMPOSE_FILE="docker-compose.local.yml"

# Check if .env file exists and suggest configuration
if [ ! -f ".env" ]; then
    echo "💡 Tip: Create a .env file for custom configuration:"
    echo "   MEM_LIMIT=12g"
    echo "   CPU_LIMIT=6"
    echo "   SPARK_PARTITIONS=16"
    echo ""
fi

# Use docker-compose for better resource management
echo "🐳 Launching with docker-compose..."
if [ "$PIPELINE" = "test" ]; then
    echo "🧪 Running test suite..."
    docker-compose -f "$COMPOSE_FILE" run --rm lastfm-test
else
    echo "🚀 Running pipeline: $PIPELINE"
    # Smart port handling
    if ! check_port_available 4040; then
        echo "⚠️  Port 4040 is in use, checking for alternatives..."
        ALTERNATIVE_PORT=$(find_available_port 4041)
        if [ -n "$ALTERNATIVE_PORT" ]; then
            export SPARK_UI_PORT="$ALTERNATIVE_PORT"
            echo "📊 Spark UI will be available at: http://localhost:$ALTERNATIVE_PORT"
        else
            echo "⚠️  Running without Spark UI (no available ports)"
            export SPARK_UI_PORT=""
        fi
    else
        echo "📊 Spark UI will be available at: http://localhost:4040"
    fi
    
    # Set pipeline environment variable and run
    export PIPELINE="$PIPELINE"
    docker-compose -f "$COMPOSE_FILE" up --remove-orphans lastfm
fi

echo "✅ Pipeline completed! Check ./data/output/ for results."
