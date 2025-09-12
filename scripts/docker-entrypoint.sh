#!/bin/bash

# LastFM Session Analysis Docker Entrypoint
# Handles SBT command execution with proper argument quoting

set -e

PIPELINE=${1:-complete}
MEMORY=${2:-12288}

echo "🎵 LastFM Session Analysis - Docker Container"
echo "=============================================="
echo "Pipeline: $PIPELINE"
echo "Memory: ${MEMORY}m"
echo "Java Version: $(java -version 2>&1 | head -n 1)"
echo ""

if [ "$PIPELINE" = "test" ]; then
    echo "🧪 Running test suite..."
    exec sbt -mem $MEMORY test
else
    echo "🚀 Running pipeline: $PIPELINE"
    # CRITICAL: Proper SBT command quoting for runMain
    exec sbt -mem $MEMORY "runMain com.lastfm.sessions.Main $PIPELINE"
fi
