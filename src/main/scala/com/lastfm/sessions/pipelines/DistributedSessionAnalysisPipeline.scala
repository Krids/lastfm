package com.lastfm.sessions.pipelines

import com.lastfm.sessions.domain.{DistributedSessionAnalysis, SessionMetrics}
import com.lastfm.sessions.infrastructure.SparkDistributedSessionAnalysisRepository
import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

/**
 * Production-grade Distributed Session Analysis Pipeline for Silver → Gold transformation.
 * 
 * Replaces the memory-intensive SessionAnalysisPipeline with a distributed processing
 * approach that can handle large datasets (19M+ records) without OutOfMemoryError.
 * 
 * Key Features:
 * - Memory-efficient distributed processing (no driver-side data collection)
 * - Spark DataFrame operations with window functions for session calculation
 * - Clean architecture with dependency injection
 * - Production-ready error handling and monitoring
 * - Comprehensive logging and performance metrics
 * 
 * Performance Characteristics:
 * - Handles datasets of any size without memory constraints
 * - Linear scaling with cluster resources
 * - Optimal partitioning for session analysis workloads
 * - Lazy evaluation with strategic caching
 * 
 * @param config Pipeline configuration with paths and optimization settings
 * @param spark Implicit Spark session for distributed processing
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DistributedSessionAnalysisPipeline(val config: PipelineConfig)(implicit spark: SparkSession) {

  /**
   * Execute distributed Silver → Gold session analysis transformation.
   * 
   * Processing Pipeline:
   * 1. **Service Creation**: Create distributed session analysis service with optimal configuration
   * 2. **Data Loading**: Load validated listening events from Silver layer as distributed stream
   * 3. **Session Calculation**: Apply 20-minute gap algorithm using Spark window functions
   * 4. **Metrics Aggregation**: Calculate comprehensive session metrics distributively
   * 5. **Gold Layer Persistence**: Generate structured artifacts for downstream consumption
   * 
   * Memory Optimization:
   * - Uses Spark DataFrames throughout (no Scala collections)
   * - Window functions for session calculation (no groupBy collect)
   * - Single-pass aggregations for metrics calculation
   * - Distributed writes without driver bottlenecks
   * 
   * @return Try containing DistributedSessionAnalysis results or comprehensive error information
   */
  def execute(): Try[DistributedSessionAnalysis] = {
    try {
      println(s"🔄 Starting Distributed Session Analysis Pipeline: Silver → Silver + Gold")
      println(s"   Clean Events Path: ${config.silverPath}")
      println(s"   Sessions Output: ${deriveSilverSessionsPath}")
      println(s"   Gold Analytics Path: ${deriveGoldPath}")
      println(s"   Session Gap: 20 minutes (distributed window functions)")
      
      // Step 1: Create sessions from clean events and save to Silver layer
      println("📊 Step 1: Creating sessions from Silver events...")
      val repository = new SparkDistributedSessionAnalysisRepository()
      val sessionsResult = repository.createAndPersistSessions(config.silverPath, deriveSilverSessionsPath)
      
      sessionsResult match {
        case Success(sessionMetrics) =>
          println("✅ Sessions created and persisted to Silver layer")
          
          // Step 2: Create analytics and persist to Gold layer
          println("🏆 Step 2: Creating analytics for Gold layer...")
          val analysis = createComprehensiveAnalysis(sessionMetrics)
          
          // Step 3: Create Gold layer structure
          repository.persistAnalysis(analysis, deriveGoldPath) match {
            case Success(_) =>
              println("✅ Gold layer structure prepared")
              
              // Step 4: Generate JSON report with all metrics and analytics
              generateSessionAnalysisJSON(analysis, deriveGoldPath)
              println("📄 JSON report generated with comprehensive metrics and analytics")
              
              logSuccessMetrics(analysis)
              Success(analysis)
              
            case Failure(exception) =>
              logFailure(exception)
              Failure(exception)
          }
          
        case Failure(exception) =>
          logFailure(exception)
          Failure(exception)
      }
      
    } catch {
      case NonFatal(exception) =>
        val errorMsg = "Distributed Session Analysis Pipeline execution failed"
        println(s"❌ $errorMsg: ${exception.getMessage}")
        Failure(new RuntimeException(errorMsg, exception))
    }
  }
  
  /**
   * Derives Gold layer output path from configuration.
   * Follows medallion architecture naming conventions.
   */
  private def deriveGoldPath: String = {
    // Replace silver with gold and add session-analytics suffix
    val basePath = config.silverPath.replace("silver", "gold")
    if (basePath.endsWith(".parquet")) {
      basePath.replace(".parquet", "-session-analytics")
    } else {
      s"${basePath}-session-analytics"
    }
  }
  
  /**
   * Derives Silver layer sessions output path.
   * Sessions are stored in Silver layer for downstream processing.
   */
  private def deriveSilverSessionsPath: String = {
    "data/output/silver/sessions.parquet"
  }
  
  /**
   * Creates comprehensive distributed session analysis from metrics.
   * The analysis uses business rules defined in the domain model.
   */
  private def createComprehensiveAnalysis(metrics: SessionMetrics): DistributedSessionAnalysis = {
    // Create analysis - domain model handles quality assessment and performance categorization
    DistributedSessionAnalysis(metrics)
  }
  
  /**
   * Generates JSON report for session analysis pipeline.
   * Follows the same pattern as data-cleaning quality reports.
   */
  private def generateSessionAnalysisJSON(analysis: DistributedSessionAnalysis, goldPath: String): Unit = {
    val reportPath = s"$goldPath/session-analysis-report.json"
    val reportContent = generateSessionAnalysisReportJSON(analysis)
    
    // Ensure Gold directory exists
    val outputFile = Paths.get(reportPath)
    val outputDir = outputFile.getParent
    if (outputDir != null) {
      Files.createDirectories(outputDir)
    }
    
    Files.write(outputFile, reportContent.getBytes(StandardCharsets.UTF_8))
    println(s"📄 Session analysis JSON report generated: $reportPath")
  }
  
  /**
   * Creates JSON content for session analysis report.
   */
  private def generateSessionAnalysisReportJSON(analysis: DistributedSessionAnalysis): String = {
    s"""{
  "timestamp": "${java.time.Instant.now()}",
  "pipeline": "session-analysis",
  "processing": {
    "totalSessions": ${analysis.metrics.totalSessions},
    "uniqueUsers": ${analysis.metrics.uniqueUsers},
    "totalTracks": ${analysis.metrics.totalTracks},
    "averageSessionLength": ${analysis.metrics.averageSessionLength},
    "qualityScore": ${analysis.metrics.qualityScore}
  },
  "qualityAssessment": "${analysis.qualityAssessment}",
  "performanceCategory": "${analysis.performanceCategory}",
  "ecosystem": {
    "isSuccessfulEcosystem": ${analysis.isSuccessfulEcosystem},
    "userActivityScore": ${analysis.userActivityScore}
  },
  "architecture": {
    "silverLayerSessions": "data/output/silver/sessions.parquet",
    "goldLayerAnalytics": "${deriveGoldPath}",
    "partitioningStrategy": "16 partitions, ~62 users per partition",
    "sessionGapAlgorithm": "20-minute window functions"
  },
  "thresholds": {
    "excellentQuality": 95.0,
    "goodQuality": 85.0,
    "acceptableQuality": 70.0,
    "highVolumeThreshold": 10000,
    "mediumVolumeThreshold": 1000
  }
}"""
  }
  
  /**
   * Logs successful execution metrics with comprehensive details.
   */
  private def logSuccessMetrics(analysis: DistributedSessionAnalysis): Unit = {
    println("✅ Distributed Session Analysis Pipeline completed successfully")
    println("📊 Session Analysis Results:")
    println(f"   Total Sessions: ${analysis.metrics.totalSessions}")
    println(f"   Unique Users: ${analysis.metrics.uniqueUsers}")
    println(f"   Total Tracks: ${analysis.metrics.totalTracks}")
    println(f"   Average Session Length: ${analysis.metrics.averageSessionLength}%.1f tracks")
    println(f"   Quality Score: ${analysis.metrics.qualityScore}%.2f%%")
    println(f"   Quality Assessment: ${analysis.qualityAssessment}")
    println(f"   Performance Category: ${analysis.performanceCategory}")
    
    // Business insights
    if (analysis.isSuccessfulEcosystem) {
      println("🎉 Analysis indicates a successful listening ecosystem!")
    }
    
    println(s"💾 JSON report with all metrics saved at: ${deriveGoldPath}/session-analysis-report.json")
    println("🚀 Ready for downstream processing and ranking analysis")
  }
  
  /**
   * Logs execution failures with actionable troubleshooting guidance.
   */
  private def logFailure(exception: Throwable): Unit = {
    println(s"❌ Session analysis failed: ${exception.getMessage}")
    
    // Provide actionable troubleshooting guidance
    println("🛠️  Troubleshooting Guidance:")
    exception match {
      case _: RuntimeException if exception.getMessage.contains("Silver layer") =>
        println("   💡 Verify Silver layer data exists and is accessible")
        println("   💡 Check data format and schema compatibility")
        
      case _: RuntimeException if exception.getMessage.contains("Gold layer") =>
        println("   💡 Verify Gold layer directory permissions")
        println("   💡 Check available disk space for output")
        
      case _ if exception.getMessage.contains("memory") || exception.getMessage.contains("Memory") =>
        println("   💡 This should not happen with distributed processing!")
        println("   💡 Check Spark executor memory configuration")
        
      case _ =>
        println("   💡 Check Spark cluster resources and connectivity")
        println("   💡 Verify data quality and format consistency")
    }
  }
}

/**
 * Companion object for factory methods and utilities.
 */
object DistributedSessionAnalysisPipeline {
  
  /**
   * Creates pipeline with production-optimized configuration.
   * 
   * @param config Pipeline configuration
   * @param spark Spark session for distributed processing
   * @return Configured DistributedSessionAnalysisPipeline
   */
  def createProduction(config: PipelineConfig)(implicit spark: SparkSession): DistributedSessionAnalysisPipeline = {
    new DistributedSessionAnalysisPipeline(config)
  }
  
  /**
   * Creates pipeline for testing with simplified configuration.
   * 
   * @param config Test configuration
   * @param spark Test Spark session
   * @return Test-configured DistributedSessionAnalysisPipeline
   */
  def createForTesting(config: PipelineConfig)(implicit spark: SparkSession): DistributedSessionAnalysisPipeline = {
    new DistributedSessionAnalysisPipeline(config)
  }
}
