package com.lastfm.sessions

import scala.util.{Success, Failure}
import java.time.LocalDateTime

// Import orchestration classes for pipeline execution
import com.lastfm.sessions.orchestration.{PipelineOrchestrator, ProductionConfigManager, PipelineExecutionResult}

/**
 * Last.fm Session Analysis - Production Main Application
 * 
 * Enterprise-grade pipeline orchestration with multi-context architecture:
 * 
 * AVAILABLE PIPELINES:
 * - data-cleaning: Bronze → Silver data quality transformation
 * - session-analysis: Silver → Gold session calculation (future)
 * - ranking: Gold → Results top songs ranking (future)
 * - complete: Full pipeline execution (default)
 * 
 * COMMAND-LINE INTERFACE:
 * sbt "runMain com.lastfm.sessions.Main"                    # Complete pipeline
 * sbt "runMain com.lastfm.sessions.Main data-cleaning"      # Data cleaning only
 * sbt "runMain com.lastfm.sessions.Main session-analysis"   # Session analysis only
 * sbt "runMain com.lastfm.sessions.Main ranking"            # Ranking only
 * 
 * PRODUCTION FEATURES:
 * - Environment-aware Spark configuration with optimal resource allocation
 * - Medallion architecture with Bronze/Silver/Gold layer separation
 * - Strategic caching through persistent artifacts for performance optimization  
 * - Comprehensive error handling with graceful failure modes and user guidance
 * - Quality monitoring with detailed metrics and audit trails
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object Main extends App {

  private val startTime = System.currentTimeMillis()

  // Production application header
  println("🎵 Last.fm Session Analysis - Production Pipeline Orchestration")
  println("=" * 90)
  println(s"🕐 Started: ${LocalDateTime.now()}")
  println(s"☕ Java Version: ${System.getProperty("java.version")}")
  println(s"🖥️  Available Cores: ${Runtime.getRuntime.availableProcessors()}")
  println(s"💾 Available Memory: ${Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024}GB")

  try {
    // Load production configuration
    val productionConfig = ProductionConfigManager.loadProductionConfig()
    
    // Display pipeline selection
    val selectedPipeline = args.headOption.getOrElse("complete")
    println(s"🎯 Selected Pipeline: '$selectedPipeline'")
    
    // Validate production prerequisites
    ProductionConfigManager.validateProductionPrerequisites(productionConfig)
    
    // Execute selected pipeline through orchestrator
    val executionResult = PipelineOrchestrator.parseArgsAndExecute(args, productionConfig)
    
    // Handle execution results
    handleExecutionResult(executionResult)
    
  } catch {
    case ex: Exception =>
      handleProductionFailure(ex)
  }

  // Display execution summary
  displayExecutionSummary()

  /**
   * Handles pipeline execution results with appropriate user feedback.
   */
  private def handleExecutionResult(result: PipelineExecutionResult): Unit = {
    result match {
      case PipelineExecutionResult.DataCleaningCompleted =>
        println("\n✅ Data Cleaning Pipeline completed successfully!")
        println("   Silver layer artifacts generated and ready for Session Analysis Context")
        
      case PipelineExecutionResult.SessionAnalysisCompleted =>
        println("\n✅ Session Analysis Pipeline completed successfully!")
        println("   Gold layer session data ready for Ranking Context")
        
      case PipelineExecutionResult.RankingCompleted =>
        println("\n✅ Ranking Pipeline completed successfully!")  
        println("   Final top_songs.tsv generated")
        
      case PipelineExecutionResult.CompletePipelineCompleted =>
        println("\n🎉 Complete Pipeline executed successfully!")
        println("   All contexts completed: Data Quality → Session Analysis → Ranking")
        println("   Final results available in Gold layer")
        
      case PipelineExecutionResult.InvalidArguments =>
        println("\n⚠️  Invalid pipeline specified. See usage help above.")
        sys.exit(1)
        
      case PipelineExecutionResult.ExecutionFailed =>
        println("\n❌ Pipeline execution failed. See error details above.")
        sys.exit(1)
    }
  }

  /**
   * Handles production pipeline failures with comprehensive error reporting.
   */
  private def handleProductionFailure(exception: Exception): Unit = {
    val executionTime = System.currentTimeMillis() - startTime
    
    println(s"\n❌ PRODUCTION PIPELINE FAILURE")
    println("-" * 80)
    println(s"🕐 Failed after: ${executionTime / 1000.0} seconds")
    println(s"🔍 Error Type: ${exception.getClass.getSimpleName}")
    println(s"📝 Error Message: ${exception.getMessage}")
    
    // Provide actionable troubleshooting guidance
    println(s"\n🛠️  Troubleshooting Guidance:")
    exception match {
      case _: RuntimeException if exception.getMessage.contains("not found") =>
        println(s"   💡 Verify Last.fm dataset is extracted to data/lastfm/lastfm-dataset-1k/")
        println(s"   💡 Check file permissions and directory access")
        
      case _: IllegalArgumentException =>
        println(s"   💡 Check configuration parameters and data paths")
        println(s"   💡 Verify medallion architecture directory structure")
        
      case _ if exception.getMessage.contains("memory") || exception.getMessage.contains("Memory") =>
        println(s"   💡 Increase JVM heap size: sbt -J-Xmx8g runMain com.lastfm.sessions.Main")
        println(s"   💡 Consider reducing Spark partition count for available memory")
        
      case _ =>
        println(s"   💡 Check system resources and Spark configuration")
        println(s"   💡 Verify Java 11 compatibility and Spark dependencies")
    }
    
    println(s"\n📋 Full Exception Stack Trace:")
    exception.printStackTrace()
    
    sys.exit(1)
  }

  /**
   * Displays comprehensive execution summary with performance metrics.
   */
  private def displayExecutionSummary(): Unit = {
    val totalExecutionTime = System.currentTimeMillis() - startTime
    val executionMinutes = totalExecutionTime / 60000.0
    
    println(s"\n📊 Execution Summary:")
    println(f"   ⏱️  Total Duration: ${executionMinutes}%.2f minutes (${totalExecutionTime / 1000.0}%.1f seconds)")
    println(s"   🎯 Pipeline: ${args.headOption.getOrElse("complete")}")
    println(s"   ✅ Status: Successfully completed")
    
    // Next steps guidance based on executed pipeline
    args.headOption.getOrElse("complete") match {
      case "data-cleaning" =>
        println(s"\n🚀 Next Steps:")
        println(s"   📄 Silver layer artifacts ready at: data/output/silver/")
        println(s"   🎯 Next: Implement Session Analysis Context")
        println(s"   🎯 Run: sbt \"runMain com.lastfm.sessions.Main session-analysis\"")
        
      case "session-analysis" =>
        println(s"\n🚀 Next Steps:")
        println(s"   📄 Gold layer session data ready for ranking")
        println(s"   🎯 Run: sbt \"runMain com.lastfm.sessions.Main ranking\"")
        
      case "ranking" =>
        println(s"\n🎯 Final Results:")
        println(s"   📄 Check: data/output/gold/top_songs.tsv")
        
      case "complete" | _ =>
        println(s"\n🎉 Complete Analysis Ready!")
        println(s"   📊 Data Quality Context: ✅ Complete")
        println(s"   🔄 Session Analysis Context: 🔄 Ready for implementation")
        println(s"   🏆 Ranking Context: ⏭️ Planned")
    }
    
    println("=" * 90)
  }
}