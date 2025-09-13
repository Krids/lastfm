#!/bin/bash

# LastFM Session Analysis Docker Entrypoint
# Handles SBT command execution with environment-based configuration

set -e

# Configuration from environment variables
PIPELINE=${1:-complete}
ENV_NAME=${ENV:-development}
JAVA_HEAP=$(echo "${JAVA_OPTS:-"-Xmx8g"}" | grep -oP '(?<=-Xmx)\d+[gG]?' | head -1)
MEMORY_MB=$(echo "${JAVA_HEAP:-8g}" | sed 's/g/*1024/; s/G/*1024/' | bc 2>/dev/null || echo 8192)

echo "🎵 LastFM Session Analysis - Docker Container"
echo "=============================================="
echo "Environment: $ENV_NAME"
echo "Pipeline: $PIPELINE"
echo "Java Heap: $JAVA_HEAP"
echo "SBT Memory: ${MEMORY_MB}m"
echo "Spark Partitions: ${SPARK_PARTITIONS:-16}"
echo "Java Version: $(java -version 2>&1 | head -n 1)"
echo "Available CPU cores: $(nproc)"
echo "Available Memory: $(free -h | grep '^Mem:' | awk '{print $2}')"
echo ""

# Health check function for container monitoring
health_check() {
    if pgrep -f "java.*Main" > /dev/null; then
        echo "✅ Application is running"
        return 0
    else
        echo "❌ Application is not running"
        return 1
    fi
}

# Handle special commands
case "$PIPELINE" in
    "health")
        health_check
        exit $?
        ;;
    "test")
        echo "🧪 Running test suite..."
        echo "Test Configuration:"
        echo "  - Memory: 2g"
        echo "  - Partitions: ${SPARK_PARTITIONS:-2}"
        echo "  - Environment: test"
        exec sbt $SBT_OPTS -mem 2048 test
        ;;
    "shell"|"console")
        echo "🐚 Starting interactive Scala console..."
        exec sbt $SBT_OPTS -mem $MEMORY_MB console
        ;;
    *)
        echo "🚀 Running pipeline: $PIPELINE"
        echo "Production Configuration:"
        echo "  - Heap: $JAVA_HEAP"
        echo "  - Partitions: ${SPARK_PARTITIONS:-16}"
        echo "  - Session Gap: ${SESSION_GAP_MINUTES:-20} minutes"
        echo "  - Top Sessions: ${TOP_SESSIONS:-50}"
        echo "  - Top Tracks: ${TOP_TRACKS:-10}"
        
        # CRITICAL: Proper SBT command quoting for runMain with environment settings
        exec sbt $SBT_OPTS -mem $MEMORY_MB "runMain com.lastfm.sessions.Main $PIPELINE"
        ;;
esac
