package com.lastfm.sessions.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.util.{Try, Success, Failure}
import scala.concurrent.duration._
import java.io.IOException
import java.net.SocketException

class RetryPolicySpec extends AnyWordSpec with Matchers {
  
  "ExponentialBackoffRetry" should {
    
    "execute operation successfully on first attempt" in {
      val retry = new ExponentialBackoffRetry(maxAttempts = 3)
      var attempts = 0
      
      val result = retry.execute {
        attempts += 1
        "success"
      }
      
      result should be(Success("success"))
      attempts should be(1)
    }
    
    "retry operation on failure and eventually succeed" in {
      val retry = new ExponentialBackoffRetry(
        maxAttempts = 3,
        initialDelay = 10.milliseconds
      )
      var attempts = 0
      
      val result = retry.execute {
        attempts += 1
        if (attempts < 3) {
          throw new RuntimeException(s"Attempt $attempts failed")
        }
        "success after retries"
      }
      
      result should be(Success("success after retries"))
      attempts should be(3)
    }
    
    "fail after max attempts exceeded" in {
      val retry = new ExponentialBackoffRetry(
        maxAttempts = 2,
        initialDelay = 10.milliseconds
      )
      var attempts = 0
      
      val result = retry.execute {
        attempts += 1
        throw new RuntimeException(s"Always fails")
      }
      
      result.isFailure should be(true)
      result.failed.get.getMessage should include("Always fails")
      attempts should be(2)
    }
    
    "apply exponential backoff between retries" in {
      val retry = new ExponentialBackoffRetry(
        maxAttempts = 3,
        initialDelay = 10.milliseconds,
        backoffMultiplier = 2.0
      )
      var attempts = 0
      val startTime = System.currentTimeMillis()
      
      retry.execute {
        attempts += 1
        if (attempts < 3) {
          throw new RuntimeException("Fail to test backoff")
        }
        "success"
      }
      
      val elapsed = System.currentTimeMillis() - startTime
      // Should take at least 10ms + 20ms = 30ms
      elapsed should be >= 25L // Allow some margin
      attempts should be(3)
    }
    
    "respect maximum delay limit" in {
      val retry = new ExponentialBackoffRetry(
        maxAttempts = 3,
        initialDelay = 10.milliseconds,
        backoffMultiplier = 100.0, // Very high multiplier
        maxDelay = 20.milliseconds // But capped at 20ms
      )
      var attempts = 0
      val startTime = System.currentTimeMillis()
      
      retry.execute {
        attempts += 1
        if (attempts < 3) {
          throw new RuntimeException("Fail to test max delay")
        }
        "success"
      }
      
      val elapsed = System.currentTimeMillis() - startTime
      // Should take 10ms + 20ms (capped) = 30ms
      elapsed should be < 50L // Should not exceed significantly
      attempts should be(3)
    }
    
    "only retry specific exceptions when configured" in {
      val retry = new ExponentialBackoffRetry(
        maxAttempts = 3,
        initialDelay = 10.milliseconds,
        retryableExceptions = Set(classOf[IOException])
      )
      
      // Should retry IOException
      var ioAttempts = 0
      val ioResult = retry.execute {
        ioAttempts += 1
        if (ioAttempts < 2) {
          throw new IOException("IO error")
        }
        "io success"
      }
      ioResult should be(Success("io success"))
      ioAttempts should be(2)
      
      // Should not retry RuntimeException
      var rtAttempts = 0
      val rtResult = retry.execute {
        rtAttempts += 1
        throw new RuntimeException("Runtime error")
      }
      rtResult.isFailure should be(true)
      rtAttempts should be(1) // No retry
    }
    
    "call error handler on failures" in {
      val retry = new ExponentialBackoffRetry(
        maxAttempts = 3,
        initialDelay = 10.milliseconds
      )
      var errorCalls = List.empty[(String, Int)]
      
      retry.executeWithHandler {
        throw new RuntimeException("Test error")
      } { (error, attempt) =>
        errorCalls = errorCalls :+ (error.getMessage, attempt)
      }
      
      errorCalls should have size 3
      errorCalls.map(_._1).distinct should be(List("Test error"))
      errorCalls.map(_._2) should be(List(1, 2, 3))
    }
    
    "validate configuration parameters" in {
      an[IllegalArgumentException] should be thrownBy {
        new ExponentialBackoffRetry(maxAttempts = 0)
      }
      
      an[IllegalArgumentException] should be thrownBy {
        new ExponentialBackoffRetry(backoffMultiplier = 0.5)
      }
      
      an[IllegalArgumentException] should be thrownBy {
        new ExponentialBackoffRetry(initialDelay = 0.milliseconds)
      }
      
      an[IllegalArgumentException] should be thrownBy {
        new ExponentialBackoffRetry(
          initialDelay = 10.seconds,
          maxDelay = 5.seconds
        )
      }
    }
  }
  
  "SimpleRetry" should {
    
    "use fixed delay between retries" in {
      val retry = new SimpleRetry(
        maxAttempts = 3,
        delay = 10.milliseconds
      )
      var attempts = 0
      val startTime = System.currentTimeMillis()
      
      retry.execute {
        attempts += 1
        if (attempts < 3) {
          throw new RuntimeException("Fail to test fixed delay")
        }
        "success"
      }
      
      val elapsed = System.currentTimeMillis() - startTime
      // Should take 10ms + 10ms = 20ms (fixed delay)
      elapsed should be >= 15L
      elapsed should be < 40L
      attempts should be(3)
    }
  }
  
  "NoRetry" should {
    
    "not retry on failure" in {
      var attempts = 0
      
      val result = NoRetry.execute {
        attempts += 1
        throw new RuntimeException("Should not retry")
      }
      
      result.isFailure should be(true)
      attempts should be(1)
    }
    
    "return success immediately" in {
      var attempts = 0
      
      val result = NoRetry.execute {
        attempts += 1
        "immediate success"
      }
      
      result should be(Success("immediate success"))
      attempts should be(1)
    }
    
    "call error handler once on failure" in {
      var errorCalled = false
      
      NoRetry.executeWithHandler {
        throw new RuntimeException("Test error")
      } { (error, attempt) =>
        errorCalled = true
        attempt should be(1)
      }
      
      errorCalled should be(true)
    }
  }
  
  "RetryPolicy factory" should {
    
    "create policy from configuration" in {
      import com.lastfm.sessions.config.AppConfiguration
      
      // Mock configuration with retries enabled
      val mockPort = new RetryMockConfigurationPort(Map(
        "pipeline.retry.max.attempts" -> 5,
        "pipeline.retry.delay.ms" -> 100,
        "pipeline.retry.backoff.multiplier" -> 1.5
      ))
      val config = AppConfiguration.withPort(mockPort)
      
      val retry = RetryPolicy.fromConfig(config)
      retry shouldBe a[ExponentialBackoffRetry]
      
      // Mock configuration with retries disabled
      val noRetryPort = new RetryMockConfigurationPort(Map(
        "pipeline.retry.max.attempts" -> 0
      ))
      val noRetryConfig = AppConfiguration.withPort(noRetryPort)
      
      val noRetry = RetryPolicy.fromConfig(noRetryConfig)
      noRetry should be(NoRetry)
    }
    
    "create default retry policy" in {
      val retry = RetryPolicy.default()
      retry shouldBe a[ExponentialBackoffRetry]
    }
    
    "create Spark-specific retry policy" in {
      val retry = RetryPolicy.forSpark()
      retry shouldBe a[ExponentialBackoffRetry]
      
      // Should retry on IOException
      var attempts = 0
      retry.execute {
        attempts += 1
        if (attempts == 1) {
          throw new SocketException("Network error")
        }
        "recovered"
      } should be(Success("recovered"))
    }
    
    "create file I/O retry policy" in {
      val retry = RetryPolicy.forFileIO()
      retry shouldBe a[ExponentialBackoffRetry]
      
      // Should retry on IOException
      var attempts = 0
      retry.execute {
        attempts += 1
        if (attempts < 3) {
          throw new IOException("File not ready")
        }
        "file read"
      } should be(Success("file read"))
    }
  }
}

/**
 * Mock implementation of ConfigurationPort for testing
 */
class RetryMockConfigurationPort(values: Map[String, Any]) extends com.lastfm.sessions.config.ConfigurationPort {
  
  override def getString(path: String): Try[String] = 
    values.get(path).map(v => Success(v.toString)).getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getInt(path: String): Try[Int] = 
    values.get(path).map {
      case i: Int => Success(i)
      case v => Success(v.toString.toInt)
    }.getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getBoolean(path: String): Try[Boolean] = 
    values.get(path).map {
      case b: Boolean => Success(b)
      case v => Success(v.toString.toBoolean)
    }.getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getDouble(path: String): Try[Double] = 
    values.get(path).map {
      case d: Double => Success(d)
      case v => Success(v.toString.toDouble)
    }.getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getStringOrDefault(path: String, default: String): String = 
    values.get(path).map(_.toString).getOrElse(default)
  
  override def getIntOrDefault(path: String, default: Int): Int = 
    values.get(path).map {
      case i: Int => i
      case v => v.toString.toInt
    }.getOrElse(default)
  
  override def getBooleanOrDefault(path: String, default: Boolean): Boolean = 
    values.get(path).map {
      case b: Boolean => b
      case v => v.toString.toBoolean
    }.getOrElse(default)
  
  override def getDoubleOrDefault(path: String, default: Double): Double = 
    values.get(path).map {
      case d: Double => d
      case v => v.toString.toDouble
    }.getOrElse(default)
  
  override def hasPath(path: String): Boolean = 
    values.contains(path)
  
  override def getEnvironment(): String = 
    getStringOrDefault("environment", "development")
}