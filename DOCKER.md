# 🐳 Docker Local Development Guide

This document provides instructions for running the LastFM Session Analysis pipeline using a simple Docker container for local development.

## 🆕 Recent Improvements

**Multi-Stage Dockerfile**: ~50% smaller final image size
- Builder stage with full JDK for compilation
- Runtime stage with minimal JRE for execution
- Non-root user execution for security

**Environment-Based Configuration**: No more hardcoded settings
- Configurable memory, CPU, and Spark settings
- Create `.env` file for custom configuration
- Separate test and production configurations

**Better Monitoring**: Health checks and resource visibility
- Built-in health checks for container monitoring
- Detailed resource usage reporting
- Enhanced logging with configuration details

## ⚙️ Environment Configuration

Create a `.env` file in the project root for custom settings:

```bash
# Resource allocation
MEM_LIMIT=12g
CPU_LIMIT=6
JAVA_OPTS=-Xmx8g -XX:+UseG1GC

# Spark configuration  
SPARK_PARTITIONS=16
SPARK_UI_PORT=4040

# Pipeline parameters
SESSION_GAP_MINUTES=20
TOP_SESSIONS=50
TOP_TRACKS=10
```

## 🚀 Quick Start

### Option 1: Simple Script (Recommended)
```bash
# Run complete pipeline (recommended)
./scripts/docker-local.sh complete

# Run data cleaning only
./scripts/docker-local.sh data-cleaning

# Run session analysis only
./scripts/docker-local.sh session-analysis

# Run tests
./scripts/docker-local.sh test

# Interactive Scala console
./scripts/docker-local.sh shell
```

### Option 2: Docker Compose (Single Service)
```bash
# Run complete pipeline
docker-compose -f docker-compose.local.yml up

# Run specific pipeline
docker-compose -f docker-compose.local.yml run lastfm data-cleaning

# Run tests with optimized test container
docker-compose -f docker-compose.local.yml run lastfm-test

# Health check
docker-compose -f docker-compose.local.yml run lastfm health
```

### Option 3: Direct Docker Command
```bash
# Build image
docker build -t lastfm-local -f Dockerfile.local .

# Run pipeline
docker run --rm \
  --memory=10g \
  --cpus="6" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -p 4040:4040 \
  lastfm-local complete
```

## 📋 Pipeline Options

| Pipeline | Description | Memory Used |
|----------|-------------|------------|
| `data-cleaning` | Bronze → Silver transformation | 8g |
| `session-analysis` | Silver → Gold transformation | 8g |
| `complete` | Full pipeline (Bronze → Silver → Gold) | 8g |
| `test` | Run test suite | 4g |

## 💾 Memory Requirements

### System Requirements
- **Minimum**: 12GB system RAM (for 8GB container + OS)
- **Recommended**: 16GB+ system RAM
- **Optimal**: 32GB+ system RAM

### Why 8GB is Enough?
Our **batch processing approach** for static TSV files means:
- ✅ **No streaming overhead** - we process static files efficiently
- ✅ **Memory-efficient Spark operations** - avoid collecting entire datasets
- ✅ **Statistical sampling** - quality metrics use 1% samples, not full data
- ✅ **Optimal partitioning** - 16 partitions for distributed processing

## 📁 Volume Mounts

The Docker container uses these volume mounts:

| Host Path | Container Path | Purpose |
|-----------|---------------|---------|
| `./data` | `/app/data` | Input data and output results |
| `./logs` | `/app/logs` | Application logs |

## 🔧 JVM Optimization

The containers use optimized JVM settings:

```bash
# Production settings
JAVA_OPTS="-Xmx12g -XX:+UseG1GC -XX:+UseContainerSupport -XX:G1HeapRegionSize=16m -XX:MaxGCPauseMillis=200"

# Key optimizations:
# - G1GC for low-latency garbage collection
# - Container awareness for proper memory detection
# - Optimized heap region size for large datasets
# - Low GC pause time targets
```

## 📊 Monitoring

### Spark UI (Optional)
Access Spark UI at: http://localhost:4040

### Health Checks
The containers include health checks to monitor JVM status:
```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3
```

### Log Monitoring
```bash
# Follow container logs
docker logs -f lastfm-production

# Check application logs
tail -f logs/application.log
```

## 🚨 Troubleshooting

### OutOfMemoryError
**Problem**: Container runs out of memory
```bash
# Solutions:
1. Increase memory allocation
./scripts/docker-local.sh complete 16g

2. Use docker-compose with higher limits
3. Check system available memory: free -h
```

### Port Conflicts
**Problem**: Port 4040 already in use
```bash
# Solution: Use different port mapping
docker run -p 4041:4040 ... lastfm-local
```

### Volume Mount Issues
**Problem**: Permission errors with data directory
```bash
# Solution: Fix directory permissions
chmod 755 data/
chown -R $(whoami):$(whoami) data/
```

### Container Build Failures
**Problem**: Docker build fails
```bash
# Solutions:
1. Clean Docker cache
docker system prune -a

2. Rebuild without cache
docker build --no-cache -t lastfm-local -f Dockerfile.local .

3. Check Dockerfile.local syntax
```

## 🔍 Performance Tips

### 1. Use SSD Storage
Mount data volumes on SSD for better I/O performance.

### 2. Allocate Adequate Memory
Monitor memory usage and adjust container limits:
```bash
docker stats lastfm-production
```

### 3. CPU Allocation
Match CPU cores to your system:
```bash
# Check available cores
nproc

# Adjust in docker-compose.local.yml
cpus: '8'  # Use 8 cores
```

### 4. Disk Space
Ensure adequate disk space for:
- Input data: ~2.4GB
- Output data: ~3-4GB
- Docker images: ~2GB
- **Total recommended**: 10GB+ free space

## 🧪 Development Workflow

### 1. Code Changes
```bash
# Rebuild image after code changes
docker build -t lastfm-local -f Dockerfile.local .

# Or use docker-compose
docker-compose -f docker-compose.local.yml build
```

### 2. Testing
```bash
# Run specific tests
./scripts/docker-local.sh test

# Run full test suite with memory allocation
docker-compose -f docker-compose.local.yml up lastfm-test
```

### 3. Debugging
```bash
# Interactive container for debugging
docker run -it --rm \
  -v "$(pwd):/opt/lastfm" \
  lastfm-local \
  bash

# Inside container
sbt console  # Interactive Scala REPL
sbt test     # Run tests
```

## 📚 Additional Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Guide](https://docs.docker.com/compose/)
- [Apache Spark on Docker](https://spark.apache.org/docs/latest/running-on-docker.html)

---

**Need help?** Check the main README.md or create an issue in the project repository.
