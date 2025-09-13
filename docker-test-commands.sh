#!/bin/bash

# LastFM Docker Pipeline Testing Commands
# Run these commands when Docker Desktop is started

echo "🐳 LastFM Docker Pipeline Testing Guide"
echo "========================================"

echo "1. 🏗️ Build Docker Image:"
echo "docker build -t lastfm-local -f Dockerfile.local ."
echo ""

echo "2. 🧪 Test Docker Environment:"
echo "docker run --rm lastfm-local health"
echo ""

echo "3. 🧹 Test Data Cleaning Pipeline (Docker):"
echo "./scripts/docker-local.sh data-cleaning"
echo ""

echo "4. 📊 Test Session Analysis Pipeline (Docker):"
echo "./scripts/docker-local.sh session-analysis"  
echo ""

echo "5. 🏆 Test Ranking Pipeline (Docker):"
echo "./scripts/docker-local.sh ranking"
echo ""

echo "6. 🔄 Test Complete Pipeline (Docker):"
echo "./scripts/docker-local.sh complete"
echo ""

echo "7. 🧪 Test Docker Test Suite:"
echo "./scripts/docker-local.sh test"
echo ""

echo "8. 📊 Monitor Docker Resources:"
echo "docker stats --format 'table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}'"
echo ""

echo "9. 🔍 Validate Docker Outputs:"
echo "ls -la data/output/{silver,gold,results}/"
echo ""

echo "Expected Results:"
echo "- Silver Layer: listening-events-cleaned.parquet + sessions.parquet" 
echo "- Gold Layer: session analytics + ranking results"
echo "- Results Layer: top_songs.tsv with top 10 tracks"
echo "- Performance: Should match local results (~3-4 minutes total)"

echo ""
echo "🎯 Docker Environment Configuration:"
echo "- Memory: 12GB limit (configured in .env)"
echo "- CPU: 6 cores (configured in .env)" 
echo "- Java: 8GB heap with G1GC"
echo "- Spark: 16 partitions optimal"
echo ""
echo "✅ All Docker testing commands ready!"
