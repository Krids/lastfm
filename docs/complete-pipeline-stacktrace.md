# LastFM Session Analysis - Complete Pipeline Stacktrace

## 📋 **Overview**
This document provides a complete stacktrace/call tree showing every file, class, method, and dependency in the LastFM Session Analysis project. Use this to understand exactly how data flows through the entire system from entry point to final output.

**Total Files:** 93 (45 production + 48 test files)
**Architecture:** Clean Architecture with Hexagonal Pattern
**Data Flow:** Bronze → Silver → Gold → Results (Medallion Architecture)

---

## 🌳 **COMPLETE SYSTEM CALL TREE**

```
📱 LASTFM SESSION ANALYSIS - COMPLETE STACKTRACE
│
└── 🚀 Main.scala (object Main extends App)
    ├── System.currentTimeMillis() → startTime
    ├── Display system information (Java version, cores, memory)
    ├── 📋 ProductionConfigManager.loadProductionConfig()
    │   ├── Runtime.getRuntime.availableProcessors() → cores
    │   ├── estimatedUsers = 1000 (LastFM 1K dataset)
    │   └── PipelineConfig(
    │       ├── bronzePath: "data/input/lastfm-dataset-1k/userid-timestamp-artid-artname-traid-traname.tsv"
    │       ├── silverPath: "data/output/silver/listening-events-cleaned.parquet"
    │       ├── UserIdPartitionStrategy(userCount=1000, cores=detected)
    │       ├── QualityThresholds(sessionAnalysisMinQuality=99.0, productionMinQuality=99.9)
    │       └── SparkConfig(partitions=16, timeZone="UTC", adaptiveEnabled=true)
    │       )
    ├── 🔍 ProductionConfigManager.validateProductionPrerequisites(config)
    │   ├── Files.exists(Paths.get(config.bronzePath)) → Bronze layer validation
    │   ├── Files.size(Paths.get(config.bronzePath)) → Size validation (min 100MB)
    │   ├── Runtime.getRuntime.maxMemory() → Memory check (recommend 4GB+)
    │   └── createProductionDirectories()
    │       ├── "data/output/bronze" → Files.createDirectories()
    │       ├── "data/output/silver" → Files.createDirectories()
    │       ├── "data/output/gold" → Files.createDirectories()
    │       └── "data/output/results" → Files.createDirectories()
    │
    ├── 🎯 PipelineOrchestrator.parseArgsAndExecute(args, config)
    │   ├── args.headOption.getOrElse("complete") → Pipeline selection
    │   ├── Pattern match on pipeline type:
    │   │   ├── "data-cleaning" → 🧹 executeDataCleaningPipeline(config)
    │   │   ├── "session-analysis" → 🔄 executeSessionAnalysisPipeline(config)
    │   │   ├── "ranking" → 🏆 executeRankingPipeline(config)
    │   │   ├── "complete" → 🚀 executeCompletePipeline(config)
    │   │   └── invalidArg → displayUsageHelp() + InvalidArguments
    │   │
    │   │   ┌─────────────────────────────────────────────────────────────────┐
    │   │   │                    🧹 DATA CLEANING PIPELINE                     │
    │   │   │                    (Bronze → Silver Layer)                      │
    │   │   └─────────────────────────────────────────────────────────────────┘
    │   ├── 🧹 executeDataCleaningPipeline(config)
    │   │   ├── using(createAppropriateSparkSession()) { implicit spark =>
    │   │   │   ├── Constants.Environment.isTestEnvironment → Boolean
    │   │   │   │   ├── sys.env.get("ENV") + sys.env.get("SBT_TEST") → Environment variables
    │   │   │   │   ├── Thread.currentThread().getStackTrace → Stack analysis
    │   │   │   │   └── sys.props.get("sbt.testing") + java.class.path → JVM properties
    │   │   │   ├── IF (isTestEnvironment)
    │   │   │   │   └── createSimpleTestSession() → SparkSession.builder()
    │   │   │   │       ├── .appName("LastFM-Test-Pipeline")
    │   │   │   │       ├── .master("local[2]")
    │   │   │   │       ├── .config("spark.sql.adaptive.enabled", "true")
    │   │   │   │       ├── .config("spark.sql.shuffle.partitions", "4")
    │   │   │   │       └── .getOrCreate()
    │   │   │   └── ELSE
    │   │   │       └── SparkSessionManager.createProductionSession()
    │   │   │           ├── Production-optimized configuration
    │   │   │           ├── Memory and CPU optimization
    │   │   │           └── Environment-aware settings
    │   │   │
    │   │   ├── 🏭 DataCleaningServiceFactory.createProductionService
    │   │   │   ├── require(spark != null, "spark session cannot be null")
    │   │   │   └── new DataCleaningService()(spark)
    │   │   │
    │   │   ├── 🔧 service.cleanData(config.bronzePath, config.silverPath)
    │   │   │   ├── validateInputPaths(bronzePath, silverPath)
    │   │   │   │   ├── require(bronzePath != null && bronzePath.nonEmpty)
    │   │   │   │   └── require(silverPath != null && silverPath.nonEmpty)
    │   │   │   │
    │   │   │   ├── createOptimalPipelineConfig(bronzePath, silverPath)
    │   │   │   │   ├── Environment-aware configuration
    │   │   │   │   ├── Partitioning strategy optimization
    │   │   │   │   └── Quality threshold setup
    │   │   │   │
    │   │   │   ├── 🔄 pipeline = new DataCleaningPipeline(config)
    │   │   │   │   ├── extends DataValidator with PerformanceMonitor
    │   │   │   │   ├── require(config != null, "config cannot be null")
    │   │   │   │   └── optimizedSchema = StructType(
    │   │   │   │       ├── StructField("userId", StringType, nullable = false)
    │   │   │   │       ├── StructField("timestamp", StringType, nullable = false)
    │   │   │   │       ├── StructField("artistId", StringType, nullable = true)
    │   │   │   │       ├── StructField("artistName", StringType, nullable = false)
    │   │   │   │       ├── StructField("trackId", StringType, nullable = true)
    │   │   │   │       └── StructField("trackName", StringType, nullable = false)
    │   │   │   │       )
    │   │   │   │
    │   │   │   └── 🚀 pipeline.execute(): Try[DataQualityMetrics]
    │   │   │       ├── Configuration validation and prerequisites
    │   │   │       ├── 📥 loadRawData(config.bronzePath): Try[DataFrame]
    │   │   │       │   ├── spark.read.option("header", "false")
    │   │   │       │   ├── .option("delimiter", "\t")
    │   │   │       │   ├── .option("encoding", "UTF-8")
    │   │   │       │   ├── .schema(optimizedSchema)
    │   │   │       │   └── .csv(config.bronzePath)
    │   │   │       │
    │   │   │       ├── 🔍 validateDataQuality(rawData): DataFrame
    │   │   │       │   ├── 🔧 DataValidator.validateUserId(userId)
    │   │   │       │   │   ├── Null and empty checks
    │   │   │       │   │   ├── Constants.DataPatterns.USER_ID_PATTERN match
    │   │   │       │   │   └── Return ValidationResult[String]
    │   │   │       │   ├── 🔧 DataValidator.validateTimestamp(timestamp)
    │   │   │       │   │   ├── ISO 8601 format parsing
    │   │   │       │   │   ├── Instant.parse(timestamp)
    │   │   │       │   │   └── Return ValidationResult[Instant]
    │   │   │       │   ├── 🔧 DataValidator.validateArtistName(artistName)
    │   │   │       │   │   ├── Unicode support and trimming
    │   │   │       │   │   └── Empty string handling
    │   │   │       │   └── 🔧 DataValidator.validateTrackName(trackName)
    │   │   │       │       ├── Business rule validation
    │   │   │       │       └── Character encoding support
    │   │   │       │
    │   │   │       ├── 🔑 generateTrackKeys(validatedData): DataFrame
    │   │   │       │   ├── Track identity resolution
    │   │   │       │   ├── MBID (MusicBrainz ID) handling
    │   │   │       │   ├── Duplicate detection and removal
    │   │   │       │   └── Track key generation for downstream processing
    │   │   │       │
    │   │   │       ├── 📊 applyStrategicPartitioning(keyedData): DataFrame
    │   │   │       │   ├── UserIdPartitionStrategy implementation
    │   │   │       │   ├── .repartition(16, $"userId")
    │   │   │       │   ├── Memory efficiency optimization
    │   │   │       │   └── Eliminates shuffle in session analysis
    │   │   │       │
    │   │   │       ├── 💾 Write Silver layer as Parquet with userId partitioning
    │   │   │       │   ├── partitionedData.write
    │   │   │       │   ├── .mode("overwrite")
    │   │   │       │   ├── .partitionBy("userId")
    │   │   │       │   ├── .parquet(config.silverPath)
    │   │   │       │   └── Columnar storage with compression
    │   │   │       │
    │   │   │       └── 📈 calculateQualityMetrics(partitionedData): DataQualityMetrics
    │   │   │           ├── 🧮 MetricsCalculator.calculateQualityScore(total, valid)
    │   │   │           │   ├── Safe division (totalRecords == 0 → 100.0)
    │   │   │           │   └── (validRecords.toDouble / totalRecords) * 100.0
    │   │   │           ├── Statistical analysis and aggregation
    │   │   │           ├── Quality score computation
    │   │   │           └── DataQualityMetrics(
    │   │   │               ├── totalRecords: Long
    │   │   │               ├── validRecords: Long
    │   │   │               ├── qualityScore: Double
    │   │   │               ├── trackIdCoverage: Double
    │   │   │               └── suspiciousUserRatio: Double
    │   │   │               )
    │   │   │
    │   │   └── Result handling and metrics display
    │   │
    │   │   ┌─────────────────────────────────────────────────────────────────┐
    │   │   │                  🔄 SESSION ANALYSIS PIPELINE                   │
    │   │   │                   (Silver → Gold Layer)                        │
    │   │   └─────────────────────────────────────────────────────────────────┘
    │   ├── 🔄 executeSessionAnalysisPipeline(config)
    │   │   ├── using(createAppropriateSparkSession()) { implicit spark =>
    │   │   ├── 🔄 pipeline = new DistributedSessionAnalysisPipeline(config)
    │   │   │   ├── val config: PipelineConfig
    │   │   │   ├── implicit spark: SparkSession
    │   │   │   └── Memory-efficient distributed processing design
    │   │   │
    │   │   └── 🚀 pipeline.execute(): Try[DistributedSessionAnalysis]
    │   │       ├── Pipeline initialization and logging
    │   │       ├── 🏭 repository = new SparkDistributedSessionAnalysisRepository()
    │   │       │   └── extends DistributedSessionAnalysisRepository
    │   │       │
    │   │       ├── 🔄 sessionsResult = repository.createAndPersistSessions(
    │   │       │   │   config.silverPath, deriveSilverSessionsPath)
    │   │       │   ├── 📥 loadEventsAsDistributedStream(eventsPath): Try[DataFrame]
    │   │       │   │   ├── spark.read.parquet(eventsPath)
    │   │       │   │   ├── Distributed stream processing (no collect)
    │   │       │   │   └── Schema validation
    │   │       │   │
    │   │       │   ├── 🔄 calculateSessionsWithWindowFunctions(events): DataFrame
    │   │       │   │   ├── Window.partitionBy($"userId").orderBy($"timestamp")
    │   │       │   │   ├── 20-minute gap algorithm implementation:
    │   │       │   │   │   ├── lag($"timestamp").over(window) → previousTimestamp
    │   │       │   │   │   ├── unix_timestamp($"timestamp") - unix_timestamp(previousTimestamp)
    │   │       │   │   │   ├── when(timeDiff > 1200, 1).otherwise(0) → isNewSession
    │   │       │   │   │   └── sum($"isNewSession").over(window) → sessionId
    │   │       │   │   ├── Session boundary identification using window functions
    │   │       │   │   └── Session aggregation with groupBy($"userId", $"sessionId")
    │   │       │   │
    │   │       │   ├── 📊 aggregateSessionMetrics(sessions): Try[SessionMetrics]
    │   │       │   │   ├── Distributed aggregation (no collect operations)
    │   │       │   │   ├── sessions.agg(
    │   │       │   │   │   ├── countDistinct($"sessionId") → totalSessions
    │   │       │   │   │   ├── countDistinct($"userId") → uniqueUsers
    │   │       │   │   │   ├── sum($"trackCount") → totalTracks
    │   │       │   │   │   └── avg($"trackCount") → averageSessionLength
    │   │       │   │   │   )
    │   │       │   │   ├── Quality score calculation
    │   │       │   │   └── SessionMetrics(
    │   │       │   │       ├── totalSessions: Long
    │   │       │   │       ├── uniqueUsers: Long  
    │   │       │   │       ├── totalTracks: Long
    │   │       │   │       ├── averageSessionLength: Double
    │   │       │   │       └── qualityScore: Double
    │   │       │   │       )
    │   │       │   │
    │   │       │   └── 💾 Persist sessions to Silver layer (16 partitions)
    │   │       │       ├── sessions.repartition(16, $"userId")
    │   │       │       ├── .write.mode("overwrite")
    │   │       │       ├── .partitionBy("userId")
    │   │       │       └── .parquet(sessionsPath)
    │   │       │
    │   │       ├── 📊 analysis = createComprehensiveAnalysis(sessionMetrics)
    │   │       │   ├── DistributedSessionAnalysis(metrics)
    │   │       │   │   ├── require(metrics != null, "metrics cannot be null")
    │   │       │   │   ├── qualityAssessment: String (computed from qualityScore)
    │   │       │   │   │   ├── >= 99.0 → "Exceptional"
    │   │       │   │   │   ├── >= 95.0 → "High"
    │   │       │   │   │   ├── >= 90.0 → "Good"
    │   │       │   │   │   └── < 90.0 → "Needs Improvement"
    │   │       │   │   └── performanceCategory: String (computed from totalSessions)
    │   │       │   │       ├── >= 100000 → "Enterprise Scale"
    │   │       │   │       ├── >= 10000 → "Production Scale"
    │   │       │   │       ├── >= 1000 → "Standard Scale"
    │   │       │   │       └── < 1000 → "Development Scale"
    │   │       │   └── Domain analysis creation with business logic
    │   │       │
    │   │       ├── 💾 repository.persistAnalysis(analysis, deriveGoldPath)
    │   │       │   ├── Gold layer structure creation
    │   │       │   ├── JSON report generation with comprehensive metrics
    │   │       │   └── Distributed persistence
    │   │       │
    │   │       ├── 📄 generateSessionAnalysisJSON(analysis, goldPath)
    │   │       │   ├── JSON report with all metrics and analytics
    │   │       │   ├── Comprehensive session statistics
    │   │       │   └── File persistence to Gold layer
    │   │       │
    │   │       └── Success logging and return DistributedSessionAnalysis
    │   │
    │   │   ┌─────────────────────────────────────────────────────────────────┐
    │   │   │                    🏆 RANKING PIPELINE                         │
    │   │   │                   (Gold → Results Layer)                       │
    │   │   └─────────────────────────────────────────────────────────────────┘
    │   ├── 🏆 executeRankingPipeline(config)
    │   │   ├── using(createAppropriateSparkSession()) { implicit spark =>
    │   │   ├── 🏆 pipeline = new RankingPipeline(config)
    │   │   │   ├── val config: PipelineConfig
    │   │   │   ├── implicit spark: SparkSession
    │   │   │   ├── 🏭 repository = new SparkRankingRepository()
    │   │   │   │   └── extends RankingRepositoryPort
    │   │   │   ├── Domain Services initialization:
    │   │   │   │   ├── 🥇 sessionRanker = new SessionRanker()
    │   │   │   │   ├── 📊 trackAggregator = new TrackAggregator()
    │   │   │   │   └── 🎵 topSongsSelector = new TopSongsSelector()
    │   │   │   └── Final pipeline stage implementation
    │   │   │
    │   │   └── 🚀 pipeline.execute(): Try[RankingResult]
    │   │       ├── 📊 sessionsResult = loadAndRankSessions()
    │   │       │   ├── 📥 repository.loadSessions(silverPath): Try[List[SessionData]]
    │   │       │   │   ├── Silver layer session loading
    │   │       │   │   ├── spark.read.parquet(path)
    │   │       │   │   ├── Parquet format processing
    │   │       │   │   └── Domain object conversion to SessionData(
    │   │       │   │       ├── sessionId: String
    │   │       │   │       ├── userId: String
    │   │       │   │       ├── trackCount: Int
    │   │       │   │       ├── durationMinutes: Long
    │   │       │   │       ├── startTime: String
    │   │       │   │       ├── endTime: String
    │   │       │   │       └── tracks: List[TrackData]
    │   │       │   │       )
    │   │       │   │
    │   │       │   ├── 🥇 sessionRanker.rankSessionsByTrackCount(allSessions)
    │   │       │   │   ├── Primary ranking: track count (descending)
    │   │       │   │   ├── Secondary ranking: duration (descending) 
    │   │       │   │   ├── Tie-breaking: userId (ascending for consistency)
    │   │       │   │   └── Return List[RankedSession] with ranking positions
    │   │       │   │
    │   │       │   └── 🎯 sessionRanker.selectTopSessions(rankedSessions, 50)
    │   │       │       ├── Top 50 session selection
    │   │       │       ├── Ranking validation
    │   │       │       └── Business rule enforcement
    │   │       │
    │   │       ├── 🎵 tracksResult = loadTracksForTopSessions(topSessions)
    │   │       │   ├── Distributed track loading for top sessions only
    │   │       │   ├── Memory optimization (not loading all tracks)
    │   │       │   ├── Session-track joining
    │   │       │   └── Return List[RankedSession] with complete track details
    │   │       │
    │   │       ├── 📊 trackPopularityResult = aggregateTrackPopularity(topSessionsWithTracks)
    │   │       │   ├── 📊 trackAggregator.aggregateTrackPopularity(sessions)
    │   │       │   │   ├── Track play counting across sessions
    │   │       │   │   ├── Session distribution analysis
    │   │       │   │   ├── User diversity metrics calculation
    │   │       │   │   └── TrackPopularity(
    │   │       │   │       ├── track: Track
    │   │       │   │       ├── playCount: Int
    │   │       │   │       ├── sessionCount: Int
    │   │       │   │       ├── userCount: Int
    │   │       │   │       └── ranking: Int
    │   │       │   │       )
    │   │       │   ├── Cross-session track counting
    │   │       │   ├── Statistical aggregation
    │   │       │   └── Return List[TrackPopularity]
    │   │       │
    │   │       ├── 🎵 topTracks = topSongsSelector.selectTopSongs(allTracks, 10)
    │   │       │   ├── 📊 rankByPopularity(tracks): List[TrackPopularity]
    │   │       │   │   ├── Play count ranking (primary)
    │   │       │   │   ├── Secondary criteria (sessionCount, userCount)
    │   │       │   │   └── Consistent deterministic ordering
    │   │       │   ├── Final ranking application
    │   │       │   ├── Top 10 selection
    │   │       │   ├── Tie-breaking logic
    │   │       │   └── Return top 10 TrackPopularity objects
    │   │       │
    │   │       ├── 📄 TSV output generation (MAIN DELIVERABLE)
    │   │       │   ├── tsvPath = "${config.outputPath}/top_songs.tsv"
    │   │       │   ├── 🏭 repository.generateTsvOutput(topTracks, tsvPath)
    │   │       │   │   ├── Column formatting (trackName, artistName, playCount)
    │   │       │   │   ├── Tab-separated values format
    │   │       │   │   ├── Single file output with coalesce(1)
    │   │       │   │   └── File system operations
    │   │       │   └── Success: "Generated top_songs.tsv"
    │   │       │
    │   │       ├── 🏆 result = RankingResult(
    │   │       │   ├── topSessions: List[RankedSession]
    │   │       │   ├── topTracks: List[TrackPopularity]
    │   │       │   ├── totalSessionsAnalyzed: Int
    │   │       │   ├── totalTracksAnalyzed: Int (sum of play counts)
    │   │       │   ├── processingTimeMillis: Long
    │   │       │   └── qualityScore: Double (99.0 default)
    │   │       │   )
    │   │       │
    │   │       ├── 💾 repository.saveRankingResults(result, goldPath)
    │   │       │   ├── Gold layer persistence
    │   │       │   ├── Structured output with metadata
    │   │       │   └── Audit trail generation
    │   │       │
    │   │       └── Success logging and return RankingResult
    │   │
    │   │   ┌─────────────────────────────────────────────────────────────────┐
    │   │   │                  🚀 COMPLETE PIPELINE                          │
    │   │   │              (Bronze → Silver → Gold → Results)                │
    │   │   └─────────────────────────────────────────────────────────────────┘
    │   └── 🚀 executeCompletePipeline(config)
    │       ├── 🧹 dataCleaningResult = executeDataCleaningPipeline(config)
    │       ├── ✅ Dependency validation (DataCleaningCompleted required)
    │       ├── 🔄 sessionAnalysisResult = executeSessionAnalysisPipeline(config)
    │       ├── ✅ Dependency validation (SessionAnalysisCompleted required)
    │       ├── 🏆 rankingResult = executeRankingPipeline(config)
    │       ├── ✅ Final validation (RankingCompleted required)
    │       └── Return CompletePipelineCompleted or throw exceptions
    │
    ├── 📊 handleExecutionResult(executionResult): Unit
    │   ├── Pattern match on PipelineExecutionResult
    │   ├── Success messaging for each pipeline type
    │   └── Error handling with sys.exit(1) for failures
    │
    └── 📈 displayExecutionSummary(): Unit
        ├── Total execution time calculation
        ├── Performance metrics display
        └── Next steps guidance based on executed pipeline

```

---

## 📁 **COMPLETE FILE DEPENDENCIES**

### **🚀 Entry Point (1 file)**
```
Main.scala
└── Application bootstrap and CLI interface
```

### **🎯 Orchestration Layer (3 files)**
```
PipelineOrchestrator.scala
├── Main pipeline dispatcher and execution control
├── Spark session management (test vs production)
└── Resource management and cleanup

ProductionConfigManager.scala
├── Configuration loading and validation
├── Environment-aware parameter optimization
└── Medallion architecture directory setup

SparkSessionManager.scala
├── Production Spark session configuration
├── Development session configuration
└── Environment-aware optimization
```

### **🏭 Application Services (2 files)**
```
DataCleaningService.scala
├── Data cleaning workflow orchestration
├── Configuration optimization
└── Quality validation and error handling

DataCleaningServiceFactory.scala
├── Production service creation
└── Spark optimization for data cleaning workloads
```

### **🔄 Pipeline Implementations (3 files)**
```
DataCleaningPipeline.scala
├── Bronze → Silver transformation
├── Strategic userId partitioning
├── Parquet output with compression
└── Comprehensive quality metrics

DistributedSessionAnalysisPipeline.scala
├── Silver → Gold transformation
├── Memory-efficient distributed processing
├── 20-minute gap session algorithm
└── JSON report generation

RankingPipeline.scala
├── Gold → Results transformation
├── Top 50 sessions ranking
├── Track popularity aggregation
└── TSV output generation (main deliverable)
```

### **🎯 Domain Models (12 files)**
```
ListenEvent.scala
├── Core listening event representation
├── Validation rules and business logic
└── Instant timestamp and Unicode support

UserSession.scala
├── User session with chronological tracks
├── Duration and track count calculations
└── Session validation logic

Track.scala
├── Track representation with MBID support
├── Track key generation
└── Artist and track validation

DataQualityMetrics.scala
├── Quality score and statistical metrics
├── Business threshold validation
└── Session analysis readiness

SessionMetrics.scala
├── Distributed session analysis results
├── Quality and performance metrics
└── Business rule validation

DistributedSessionAnalysis.scala
├── Complete session analysis results
├── Quality assessment categorization
└── Performance category determination

RankedSession.scala
├── Session with ranking metadata
├── Track count and duration ranking
└── Ranking consistency validation

TrackPopularity.scala
├── Track popularity statistics
├── Cross-session play counting
└── User diversity metrics

RankingResult.scala
├── Final ranking results with metadata
├── Processing time and quality metrics
└── Audit trail information

ValidationResult.scala
├── Type-safe validation outcomes
├── Valid/Invalid pattern matching
└── Error message aggregation

SessionBoundaryDetector.scala
├── 20-minute gap algorithm
├── Session boundary identification
└── Chronological session grouping

LastFmDataValidation.scala
├── LastFM-specific validation rules
├── Data pattern enforcement
└── Business rule validation
```

### **🎯 Domain Services (3 files)**
```
SessionRanker.scala
├── Session ranking by track count
├── Tie-breaking logic (duration, userId)
├── Top N session selection
└── Deterministic ordering

TrackAggregator.scala
├── Track popularity calculation
├── Cross-session statistics
├── User diversity analysis
└── Play count aggregation

TopSongsSelector.scala
├── Final track selection (top 10)
├── Popularity-based ranking
├── Tie-breaking consistency
└── Business rule enforcement
```

### **🔌 Repository Ports (3 files)**
```
DataRepositoryPort.scala
├── Data loading interface
├── Quality assessment contract
└── Bronze/Silver operations

DistributedSessionAnalysisRepository.scala
├── Session analysis data operations
├── Distributed processing contract
└── Memory-efficient operations

RankingRepositoryPort.scala
├── Ranking data access interface
├── Session and metadata loading
└── TSV output generation contract
```

### **🏗️ Infrastructure Adapters (3 files)**
```
SparkDataRepository.scala
├── Spark-based data operations
├── TSV parsing and Parquet writing
├── Schema validation and error handling
└── Memory-efficient batch processing

SparkDistributedSessionAnalysisRepository.scala
├── Distributed session analysis implementation
├── Window function-based session detection
├── Spark DataFrame aggregations
└── Silver/Gold layer persistence

SparkRankingRepository.scala
├── Spark-based ranking operations
├── Session and track loading
├── TSV output generation
└── Gold layer persistence
```

### **⚙️ Configuration (3 files)**
```
PipelineConfig.scala
├── Complete pipeline configuration
├── Medallion architecture paths
├── Partitioning and quality strategies
└── Spark optimization settings

AppConfiguration.scala
├── Application-wide configuration
├── Environment-aware settings
└── External configuration support

ConfigurableConstants.scala
├── Hierarchical configuration system
├── Environment variable overrides
└── Default value management
```

### **🔧 Common Traits (4 files)**
```
DataValidator.scala
├── Reusable validation patterns
├── LastFM-specific business rules
├── Unicode and timestamp support
└── Type-safe validation results

MetricsCalculator.scala
├── Quality score calculations
├── Statistical computations
├── Business readiness assessment
└── Safe mathematical operations

PerformanceMonitor.scala
├── Performance tracking patterns
├── Timing measurements
└── Resource monitoring

PipelineReporter.scala
├── JSON report generation
├── Standardized formatting
└── File I/O operations
```

### **🛠️ Common Utilities (6 files)**
```
Constants.scala
├── Application-wide constants
├── Data patterns and thresholds
└── Default configuration values

ErrorMessages.scala
├── Centralized error messages
├── Validation error formatting
└── User-friendly error guidance

RetryPolicy.scala
├── Configurable retry logic
├── Exponential backoff
├── Circuit breaker pattern
└── Exception classification

ValidationRules.scala
├── Common validation patterns
├── Business rule enforcement
└── Error message standardization

SparkPerformanceMonitor.scala
├── Spark-specific monitoring
├── Query plan analysis
└── Execution metrics

SparkConfigurable.scala
├── Spark configuration patterns
└── Environment-aware settings
```

---

## 📊 **DATA FLOW SUMMARY**

### **🔄 Processing Stages**
```
1. 🥉 Bronze Layer (Raw Data)
   └── LastFM TSV: 19,150,868 records from 1K users
       └── File: userid-timestamp-artid-artname-traid-traname.tsv

2. 🥈 Silver Layer (Clean Data) 
   └── Parquet format with userId partitioning (16 partitions)
       ├── listening-events-cleaned.parquet/
       └── listening-events-cleaned-report.json

3. 🥇 Gold Layer (Business Logic)
   └── Session analysis results and structured analytics
       ├── session-analysis-report.json
       ├── 50-largest-sessions/
       └── top-10-tracks/

4. 📊 Results Layer (Final Output)
   └── top_songs.tsv (MAIN DELIVERABLE)
       └── Top 10 most popular songs from top 50 longest sessions
```

### **🎯 Quality Gates**
```
Bronze → Silver: 99.0% minimum quality score
Silver → Gold: Session analysis readiness validation  
Gold → Results: 100% deterministic ranking
Complete Pipeline: 99.9% production quality threshold
```

### **⚡ Performance Characteristics**
```
Partitioning Strategy: 16 partitions (16 cores optimized)
Session Algorithm: 20-minute gap detection using Spark window functions
Memory Efficiency: No driver-side data collection (distributed processing)
Output Format: Single coalesced TSV file for final deliverable
Processing Scale: Handles 19M+ records without OutOfMemoryError
```

---

## 🎯 **EXECUTION COMMANDS**

### **Individual Pipeline Execution**
```bash
# Data Cleaning Pipeline Only
sbt "runMain com.lastfm.sessions.Main data-cleaning"

# Session Analysis Pipeline Only  
sbt "runMain com.lastfm.sessions.Main session-analysis"

# Ranking Pipeline Only
sbt "runMain com.lastfm.sessions.Main ranking"

# Complete Pipeline (Default)
sbt "runMain com.lastfm.sessions.Main complete"
sbt "runMain com.lastfm.sessions.Main"
```

### **Development and Testing**
```bash
# Run with increased memory for large dataset
sbt -J-Xmx8g "runMain com.lastfm.sessions.Main complete"

# Test coverage analysis
sbt coverage test coverageReport

# Individual test suites
sbt "testOnly *validation* *ErrorHandling*"
```

---

## 📈 **ARCHITECTURE SUMMARY**

- **📐 Architecture Pattern:** Clean Architecture with Hexagonal Design
- **🏗️ Data Architecture:** Medallion Architecture (Bronze/Silver/Gold/Results)  
- **⚡ Processing Engine:** Apache Spark with DataFrame API
- **🔄 Development Approach:** Test-Driven Development (TDD)
- **📊 Quality Focus:** 99%+ data quality with comprehensive validation
- **🎯 Performance:** Memory-efficient distributed processing
- **🔧 Configuration:** Environment-aware with production optimization

**Total System Complexity:** 93 files, 45 production classes, comprehensive end-to-end data pipeline with enterprise-grade quality and performance characteristics.

This stacktrace provides complete visibility into every aspect of the LastFM Session Analysis system for comprehensive code study and understanding.
