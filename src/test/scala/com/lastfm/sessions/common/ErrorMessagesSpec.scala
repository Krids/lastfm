package com.lastfm.sessions.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for centralized error message system.
 */
class ErrorMessagesSpec extends AnyFlatSpec with Matchers {
  
  "ErrorMessages.Validation" should "format validation errors consistently" in {
    ErrorMessages.Validation.emptyField("userId") should be("userId cannot be empty")
    ErrorMessages.Validation.invalidFormat("user_XXXXXX") should be("Invalid format. Expected: user_XXXXXX")
    ErrorMessages.Validation.outOfRange("score", 0.0, 100.0) should be("score must be between 0.0 and 100.0")
    ErrorMessages.Validation.tooLong("field", 50) should be("field exceeds maximum length of 50 characters")
  }
  
  it should "handle special user ID errors" in {
    ErrorMessages.Validation.invalidUserId("bad_user") should include("Invalid user ID format")
    ErrorMessages.Validation.invalidUserId("bad_user") should include("user_XXXXXX")
  }
  
  it should "handle timestamp errors" in {
    ErrorMessages.Validation.invalidTimestamp("bad-time") should include("Invalid timestamp format")
    ErrorMessages.Validation.invalidTimestamp("bad-time") should include("ISO 8601")
  }
  
  "ErrorMessages.Pipeline" should "format pipeline errors consistently" in {
    ErrorMessages.Pipeline.executionFailed("ranking", "out of memory") should be("Pipeline 'ranking' failed: out of memory")
    ErrorMessages.Pipeline.stageFailure("validation", "null pointer") should be("Stage 'validation' failed with error: null pointer")
    ErrorMessages.Pipeline.missingInput("file.tsv") should be("Input file not found: file.tsv")
  }
  
  it should "handle quality threshold errors" in {
    val error = ErrorMessages.Pipeline.qualityThresholdNotMet(85.5, 99.0)
    error should include("85.50%")
    error should include("99.00%")
    error should include("below required threshold")
  }
  
  "ErrorMessages.IO" should "format I/O errors consistently" in {
    ErrorMessages.IO.fileNotFound("missing.txt") should be("File not found: missing.txt")
    ErrorMessages.IO.directoryNotFound("missing/") should be("Directory not found: missing/")
    ErrorMessages.IO.readError("file.txt", "permission denied") should be("Failed to read from 'file.txt': permission denied")
    ErrorMessages.IO.writeError("output.txt", "disk full") should be("Failed to write to 'output.txt': disk full")
  }
  
  "ErrorMessages.Spark" should "format Spark errors consistently" in {
    ErrorMessages.Spark.outOfMemory("shuffle") should be("Out of memory during operation: shuffle")
    ErrorMessages.Spark.sessionCreationFailed("invalid config") should be("Failed to create Spark session: invalid config")
    ErrorMessages.Spark.partitionSkew(10000, 1000) should be("Severe partition skew detected. Max: 10000, Avg: 1000")
  }
  
  "ErrorMessages.withContext" should "include context information" in {
    val contextualError = ErrorMessages.withContext(
      "Processing failed",
      Map("records" -> 1000, "stage" -> "validation", "memory" -> "4GB")
    )
    
    contextualError should include("Processing failed")
    contextualError should include("records: 1000")
    contextualError should include("stage: validation")
    contextualError should include("memory: 4GB")
    contextualError should include("Context:")
  }
  
  it should "handle empty context gracefully" in {
    val error = ErrorMessages.withContext("Base error", Map.empty)
    error should be("Base error")
  }
  
  "ErrorMessages.userFriendly" should "provide user-friendly translations" in {
    ErrorMessages.userFriendly("OutOfMemoryError: Java heap space") should include("ran out of memory")
    ErrorMessages.userFriendly("FileNotFoundException: input.tsv") should include("Input file not found")
    
    // Test the actual quality threshold message format
    val qualityError = ErrorMessages.Pipeline.qualityThresholdNotMet(85.5, 99.0)
    ErrorMessages.userFriendly(qualityError) should include("Data quality is below acceptable levels")
    
    ErrorMessages.userFriendly("Permission denied") should include("Permission denied")
    ErrorMessages.userFriendly("Connection timeout") should include("timed out")
  }
  
  it should "handle unknown errors gracefully" in {
    val unknownError = "Some random technical error"
    val friendlyError = ErrorMessages.userFriendly(unknownError)
    
    friendlyError should include(unknownError)
    friendlyError should include("check the logs")
  }
  
  "ErrorMessages.structured" should "create structured error logs" in {
    val structuredError = ErrorMessages.structured(
      error = "Test error",
      category = ErrorMessages.Categories.VALIDATION,
      severity = ErrorMessages.Severity.ERROR,
      component = "TestComponent",
      context = Map("userId" -> "user_123", "records" -> 1000)
    )
    
    structuredError should include("error=Test error")
    structuredError should include("category=VALIDATION")
    structuredError should include("severity=ERROR")
    structuredError should include("component=TestComponent")
    structuredError should include("userId=user_123")
    structuredError should include("records=1000")
    structuredError should include("timestamp=")
  }
  
  "ErrorMessages categories and severity" should "provide standard values" in {
    ErrorMessages.Categories.VALIDATION should be("VALIDATION")
    ErrorMessages.Categories.PIPELINE should be("PIPELINE")
    ErrorMessages.Categories.SPARK should be("SPARK")
    
    ErrorMessages.Severity.INFO should be("INFO")
    ErrorMessages.Severity.WARN should be("WARN")
    ErrorMessages.Severity.ERROR should be("ERROR")
    ErrorMessages.Severity.FATAL should be("FATAL")
  }
}