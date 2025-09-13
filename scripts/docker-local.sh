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

# Smart port handling for non-test pipelines
if [ "$PIPELINE" != "test" ]; then
    echo "🔍 Checking port availability for Spark UI..."
    
    if check_port_available 4040; then
        PORT_FLAG="-p 4040:4040"
        echo "📊 Spark UI will be available at: http://localhost:4040"
    else
        echo "⚠️  Port 4040 is in use, checking for alternatives..."
        # Show what's using port 4040 for debugging
        PROC_USING_PORT=$(lsof -ti:4040 2>/dev/null | head -1)
        if [ -n "$PROC_USING_PORT" ]; then
            PROC_NAME=$(ps -p $PROC_USING_PORT -o comm= 2>/dev/null || echo "unknown")
            echo "   💡 Port 4040 is being used by: $PROC_NAME (PID: $PROC_USING_PORT)"
        fi
        ALTERNATIVE_PORT=$(find_available_port 4041)
        
        if [ -n "$ALTERNATIVE_PORT" ]; then
            PORT_FLAG="-p $ALTERNATIVE_PORT:4040"
            echo "📊 Spark UI will be available at: http://localhost:$ALTERNATIVE_PORT"
        else
            PORT_FLAG=""
            echo "⚠️  No available ports found (4041-4051), running without Spark UI"
            echo "💡 You can still monitor progress through console logs"
        fi
    fi
else
    PORT_FLAG=""
    echo "🧪 Running tests (no Spark UI needed)"
fi

echo "🐳 Launching Docker container..."
docker run --rm \
    --name "lastfm-runner" \
    --memory="16g" \
    --cpus="8" \
    -v "$(pwd)/data:/app/data" \
    -v "$(pwd)/logs:/app/logs" \
    $PORT_FLAG \
    "$IMAGE_NAME" \
    "$PIPELINE"

echo "✅ Pipeline completed! Check ./data/output/ for results."
