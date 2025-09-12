package com.lastfm.sessions.pipelines

import com.lastfm.sessions.application.{SessionAnalysisService, DistributedSessionAnalysisFactory}
import com.lastfm.sessions.domain.DistributedSessionAnalysis
import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

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
      println(s"🔄 Starting Distributed Session Analysis Pipeline: Silver → Gold")
      println(s"   Silver Path: ${config.silverPath}")
      println(s"   Gold Path: ${deriveGoldPath}")
      println(s"   Session Gap: 20 minutes (distributed window functions)")
      println(s"   Processing Mode: Distributed (no driver memory collection)")
      
      // Step 1: Create distributed session analysis service with optimal configuration
      println("⚙️  Step 1: Creating distributed session analysis service...")
      val service = DistributedSessionAnalysisFactory.createProductionService
      println("✅ Service created with optimal Spark configuration")
      
      // Step 2: Execute distributed session analysis workflow
      println("📊 Step 2: Executing distributed session analysis workflow...")
      val analysisResult = service.analyzeUserSessions(config.silverPath, deriveGoldPath)
      
      analysisResult match {
        case Success(analysis) =>
          logSuccessMetrics(analysis)
          Success(analysis)
          
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
    config.silverPath.replace("silver", "gold").replace(".tsv", "-distributed-sessions")
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
    
    println(s"💾 Gold layer artifacts generated at: ${deriveGoldPath}")
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
