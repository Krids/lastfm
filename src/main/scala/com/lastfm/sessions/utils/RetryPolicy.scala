package com.lastfm.sessions.utils

import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory

/**
 * Retry policy trait defining the contract for retry behavior.
 * This follows the hexagonal architecture by defining the port for retry logic.
 */
trait RetryPolicy {
  /**
   * Executes an operation with retry logic
   * @param operation The operation to execute
   * @tparam T The return type of the operation
   * @return A Try containing the result or failure
   */
  def execute[T](operation: => T): Try[T]
  
  /**
   * Executes an operation with retry logic and a custom error handler
   * @param operation The operation to execute
   * @param onError Error handler called on each failure
   * @tparam T The return type of the operation
   * @return A Try containing the result or failure
   */
  def executeWithHandler[T](operation: => T)(onError: (Throwable, Int) => Unit): Try[T]
}

/**
 * Implementation of exponential backoff retry policy
 * @param maxAttempts Maximum number of retry attempts
 * @param initialDelay Initial delay between retries
 * @param backoffMultiplier Multiplier for exponential backoff
 * @param maxDelay Maximum delay between retries
 * @param retryableExceptions Set of exceptions that trigger retries (empty = retry all)
 */
class ExponentialBackoffRetry(
  maxAttempts: Int = 3,
  initialDelay: FiniteDuration = 1.second,
  backoffMultiplier: Double = 2.0,
  maxDelay: FiniteDuration = 30.seconds,
  retryableExceptions: Set[Class[_ <: Throwable]] = Set.empty
) extends RetryPolicy {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  require(maxAttempts > 0, "maxAttempts must be positive")
  require(backoffMultiplier >= 1.0, "backoffMultiplier must be at least 1.0")
  require(initialDelay > Duration.Zero, "initialDelay must be positive")
  require(maxDelay >= initialDelay, "maxDelay must be >= initialDelay")
  
  override def execute[T](operation: => T): Try[T] = {
    executeWithHandler(operation) { (error, attempt) =>
      logger.warn(s"Operation failed on attempt $attempt: ${error.getMessage}")
    }
  }
  
  override def executeWithHandler[T](operation: => T)(onError: (Throwable, Int) => Unit): Try[T] = {
    var lastError: Throwable = null
    var currentDelay = initialDelay
    var attemptCount = 0
    
    for (attempt <- 1 to maxAttempts) {
      attemptCount = attempt
      Try(operation) match {
        case Success(result) =>
          if (attempt > 1) {
            logger.info(s"Operation succeeded after $attempt attempts")
          }
          return Success(result)
          
        case Failure(error) =>
          lastError = error
          onError(error, attempt)
          
          if (attempt < maxAttempts && shouldRetry(error)) {
            val delayMs = Math.min(currentDelay.toMillis, maxDelay.toMillis)
            logger.info(s"Retrying after ${delayMs}ms (attempt $attempt/$maxAttempts)")
            Thread.sleep(delayMs)
            currentDelay = FiniteDuration((currentDelay.toMillis * backoffMultiplier).toLong, MILLISECONDS)
          } else if (attempt < maxAttempts && !shouldRetry(error)) {
            // Don't retry if exception is not retryable
            logger.warn(s"Not retrying ${error.getClass.getSimpleName} - not in retryable exceptions")
            return Failure(lastError)
          }
      }
    }
    
    logger.error(s"Operation failed after $attemptCount attempts", lastError)
    Failure(lastError)
  }
  
  private def shouldRetry(error: Throwable): Boolean = {
    if (retryableExceptions.isEmpty) {
      // Retry all exceptions if no specific set is defined
      true
    } else {
      // Check if the exception or any of its causes match the retryable set
      retryableExceptions.exists(_.isAssignableFrom(error.getClass)) ||
        (error.getCause != null && shouldRetry(error.getCause))
    }
  }
}

/**
 * Simple retry policy without backoff
 * @param maxAttempts Maximum number of retry attempts
 * @param delay Fixed delay between retries
 */
class SimpleRetry(
  maxAttempts: Int = 3,
  delay: FiniteDuration = 1.second
) extends ExponentialBackoffRetry(
  maxAttempts = maxAttempts,
  initialDelay = delay,
  backoffMultiplier = 1.0,
  maxDelay = delay
)

/**
 * No-op retry policy that doesn't retry
 */
object NoRetry extends RetryPolicy {
  override def execute[T](operation: => T): Try[T] = Try(operation)
  
  override def executeWithHandler[T](operation: => T)(onError: (Throwable, Int) => Unit): Try[T] = {
    Try(operation) match {
      case Failure(error) =>
        onError(error, 1)
        Failure(error)
      case success => success
    }
  }
}

/**
 * Factory for creating retry policies based on configuration
 */
object RetryPolicy {
  
  /**
   * Creates a retry policy from configuration
   * @param config The application configuration
   * @return A configured retry policy
   */
  def fromConfig(config: com.lastfm.sessions.config.AppConfiguration): RetryPolicy = {
    if (config.maxRetryAttempts == 0) {
      NoRetry
    } else {
      new ExponentialBackoffRetry(
        maxAttempts = config.maxRetryAttempts,
        initialDelay = config.retryDelayMs.milliseconds,
        backoffMultiplier = config.retryBackoffMultiplier,
        maxDelay = (config.retryDelayMs * Math.pow(config.retryBackoffMultiplier, config.maxRetryAttempts - 1)).toLong.milliseconds
      )
    }
  }
  
  /**
   * Creates a default retry policy
   * @return A retry policy with sensible defaults
   */
  def default(): RetryPolicy = {
    new ExponentialBackoffRetry()
  }
  
  /**
   * Creates a retry policy for Spark operations
   * Configured with longer delays and specific retryable exceptions
   */
  def forSpark(): RetryPolicy = {
    new ExponentialBackoffRetry(
      maxAttempts = 3,
      initialDelay = 2.seconds,
      backoffMultiplier = 2.0,
      maxDelay = 30.seconds,
      retryableExceptions = Set(
        classOf[java.io.IOException],
        classOf[java.net.SocketException],
        classOf[java.net.SocketTimeoutException],
        classOf[org.apache.spark.SparkException]
      )
    )
  }
  
  /**
   * Creates a retry policy for file I/O operations
   */
  def forFileIO(): RetryPolicy = {
    new ExponentialBackoffRetry(
      maxAttempts = 5,
      initialDelay = 500.milliseconds,
      backoffMultiplier = 1.5,
      maxDelay = 5.seconds,
      retryableExceptions = Set(
        classOf[java.io.IOException],
        classOf[java.nio.file.FileSystemException]
      )
    )
  }
}