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
    // Parse arguments first to handle special commands before data validation
    val selectedPipeline = args.headOption.getOrElse("complete")
    println(s"🎯 Selected Pipeline: '$selectedPipeline'")
    
    // Handle special commands that don't require data validation
    selectedPipeline match {
      case "--help" | "help" => 
        PipelineOrchestrator.displayUsageHelp()
        sys.exit(0)
      case "--version" | "version" => 
        displayVersionInfo()
        sys.exit(0)
      case "java" =>
        displayJavaInfo()
        sys.exit(0)
      case _ =>
        // For actual pipeline execution, load config and validate prerequisites
        val productionConfig = ProductionConfigManager.loadProductionConfig()
        ProductionConfigManager.validateProductionPrerequisites(productionConfig)
        
        // Execute selected pipeline through orchestrator
        val executionResult = PipelineOrchestrator.parseArgsAndExecute(args, productionConfig)
        handleExecutionResult(executionResult)
    }
    
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

  /**
   * Displays version information for container validation.
   */
  private def displayVersionInfo(): Unit = {
    println("🎵 Last.fm Session Analysis - Version Information")
    println("=" * 60)
    println(s"📦 Application Version: 1.0.0")
    println(s"🏗️  Build Environment: Production")
    println(s"📅 Build Date: ${LocalDateTime.now()}")
    println(s"☕ Java Version: ${System.getProperty("java.version")}")
    println(s"🔧 Scala Version: ${scala.util.Properties.versionString}")
    println(s"🎯 Target Platform: Docker Container")
    println("=" * 60)
  }

  /**
   * Displays Java environment information for container validation.
   */
  private def displayJavaInfo(): Unit = {
    println("☕ Java Environment Information")
    println("=" * 40)
    println(s"Version: ${System.getProperty("java.version")}")
    println(s"Vendor: ${System.getProperty("java.vendor")}")
    println(s"Home: ${System.getProperty("java.home")}")
    println(s"VM Name: ${System.getProperty("java.vm.name")}")
    println(s"VM Version: ${System.getProperty("java.vm.version")}")
    println(s"Available Processors: ${Runtime.getRuntime.availableProcessors()}")
    println(s"Max Memory: ${Runtime.getRuntime.maxMemory() / 1024 / 1024}MB")
    println("=" * 40)
  }
}