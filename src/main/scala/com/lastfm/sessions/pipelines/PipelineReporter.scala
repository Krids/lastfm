package com.lastfm.sessions.pipelines

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Common trait for pipeline reporting functionality.
 * 
 * Provides standardized JSON report generation for all pipeline stages
 * following clean code principles and DRY (Don't Repeat Yourself).
 * 
 * Key Features:
 * - Consistent JSON structure across all pipelines
 * - Common timestamp and metadata formatting
 * - Standardized file I/O operations
 * - Error handling for report generation
 * 
 * Design Principles:
 * - Single Responsibility: Only handles report generation
 * - Open/Closed: Extensible for new report types
 * - Interface Segregation: Minimal required interface
 * - Clean Error Handling: Graceful failure modes
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait PipelineReporter {
  
  /**
   * Generates and persists a JSON report to the specified path.
   * 
   * @param reportPath Full path where the report should be saved
   * @param reportContent JSON content as a string
   * @return Try indicating success or failure of report generation
   */
  protected def persistJSONReport(reportPath: String, reportContent: String): Try[Unit] = {
    try {
      // Ensure output directory exists
      val outputFile = Paths.get(reportPath)
      val outputDir = outputFile.getParent
      if (outputDir != null) {
        Files.createDirectories(outputDir)
      }
      
      // Write report content
      Files.write(outputFile, reportContent.getBytes(StandardCharsets.UTF_8))
      Success(())
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Failed to generate report at $reportPath: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Formats a timestamp for JSON reports.
   * Uses ISO-8601 format for consistency.
   * 
   * @return Formatted timestamp string
   */
  protected def formatTimestamp(): String = {
    java.time.Instant.now().toString
  }
  
  /**
   * Creates common JSON header for all reports.
   * 
   * @param pipelineName Name of the pipeline generating the report
   * @return JSON header string
   */
  protected def createJSONHeader(pipelineName: String): String = {
    s"""{
  "timestamp": "${formatTimestamp()}",
  "pipeline": "$pipelineName","""
  }
  
  /**
   * Determines quality assessment based on score.
   * Provides consistent quality categorization across pipelines.
   * 
   * @param qualityScore Numeric quality score (0-100)
   * @return Quality assessment string
   */
  protected def assessQuality(qualityScore: Double): String = {
    qualityScore match {
      case score if score >= 99.9 => "Excellent"
      case score if score >= 99.0 => "Very Good"
      case score if score >= 95.0 => "Good"
      case score if score >= 90.0 => "Acceptable"
      case score if score >= 85.0 => "Fair"
      case score if score >= 70.0 => "Poor"
      case _ => "Needs Improvement"
    }
  }
  
  /**
   * Formats numeric values consistently for JSON output.
   * 
   * @param value Numeric value to format
   * @param decimals Number of decimal places (default: 2)
   * @return Formatted numeric string
   */
  protected def formatNumeric(value: Double, decimals: Int = 2): String = {
    val formatString = s"%.${decimals}f"
    formatString.format(value)
  }
  
  /**
   * Creates a JSON object from a map of key-value pairs.
   * 
   * @param values Map of field names to values
   * @param indent Indentation level for formatting
   * @return Formatted JSON object string
   */
  protected def createJSONObject(values: Map[String, Any], indent: Int = 2): String = {
    val indentStr = " " * indent
    values.map { case (key, value) =>
      value match {
        case s: String => s"""$indentStr"$key": "$s""""
        case n: Number => s"""$indentStr"$key": $n"""
        case b: Boolean => s"""$indentStr"$key": $b"""
        case m: Map[_, _] => s"""$indentStr"$key": {\n${createJSONObject(m.asInstanceOf[Map[String, Any]], indent + 2)}\n$indentStr}"""
        case _ => s"""$indentStr"$key": "${value.toString}""""
      }
    }.mkString(",\n")
  }
  
  /**
   * Abstract method for generating pipeline-specific report content.
   * Must be implemented by each pipeline.
   * 
   * @return JSON report content as string
   */
  def generateReportJSON(): String
  
  /**
   * Abstract method for getting the report output path.
   * Must be implemented by each pipeline.
   * 
   * @return Full path for the report file
   */
  def getReportPath(): String
  
  /**
   * Template method for generating and persisting reports.
   * Combines common functionality with pipeline-specific implementations.
   * 
   * @return Try indicating success or failure
   */
  def generateAndPersistReport(): Try[Unit] = {
    val reportPath = getReportPath()
    val reportContent = generateReportJSON()
    persistJSONReport(reportPath, reportContent)
  }
}