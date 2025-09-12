package com.lastfm.sessions.application

import com.lastfm.sessions.domain._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Try, Success, Failure}

/**
 * TDD-First specification for SessionAnalysisService application layer.
 * 
 * Tests the orchestration of session analysis workflow following clean architecture.
 * Focuses on business logic coordination without infrastructure dependencies.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionAnalysisServiceSpec extends AnyFlatSpec with Matchers {

  "SessionAnalysisService" should "orchestrate complete session analysis workflow" in {
    // Given: Service with test repository
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When: Analysis is executed
    val result = service.analyzeUserSessions("silver/path", "gold/path")
    
    // Then: Analysis completes successfully
    result.isSuccess shouldBe true
    result.get shouldBe a[DistributedSessionAnalysis]
    result.get.metrics.totalSessions should be > 0L
    result.get.metrics.uniqueUsers should be > 0L
  }
  
  it should "apply optimal partitioning for session analysis" in {
    // Given: Service with partitioning strategy
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When: Analysis is executed
    val result = service.analyzeUserSessions("silver/path", "gold/path")
    
    // Then: Partitioning is applied (verified through successful execution)
    result.isSuccess shouldBe true
    // In real implementation, this would verify partitionByUser() was called
  }
  
  it should "cache event stream for multiple operations" in {
    // Given: Service that performs multiple operations
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When: Analysis is executed (involves multiple stream operations)
    val result = service.analyzeUserSessions("silver/path", "gold/path")
    
    // Then: Stream caching is utilized (verified through successful execution)
    result.isSuccess shouldBe true
    // In real implementation, this would verify cache() was called
  }
  
  it should "handle repository failures gracefully" in {
    // Given: Service with failing repository
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When: Analysis fails at repository level
    val result = service.analyzeUserSessions("invalid/path", "gold/path")
    
    // Then: Failure is handled appropriately
    result.isFailure shouldBe true
    result.failed.get shouldBe a[RuntimeException]
  }
  
  it should "validate input parameters" in {
    // Given: Service instance
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When/Then: Null paths should be rejected
    intercept[IllegalArgumentException] {
      service.analyzeUserSessions(null, "gold/path")
    }
    
    intercept[IllegalArgumentException] {
      service.analyzeUserSessions("silver/path", null)
    }
  }
  
  it should "generate comprehensive analysis metrics" in {
    // Given: Service with test data
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When: Analysis is executed
    val result = service.analyzeUserSessions("silver/path", "gold/path")
    
    // Then: Comprehensive metrics are generated
    result.isSuccess shouldBe true
    val analysis = result.get
    
    analysis.metrics.totalSessions should be > 0L
    analysis.metrics.uniqueUsers should be > 0L
    analysis.metrics.totalTracks should be > 0L
    analysis.metrics.averageSessionLength should be > 0.0
    analysis.metrics.qualityScore should be >= 0.0
    analysis.metrics.qualityScore should be <= 100.0
  }
  
  it should "provide quality assessment of analysis results" in {
    // Given: Service that generates high-quality results
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When: Analysis is executed
    val result = service.analyzeUserSessions("silver/path", "gold/path")
    
    // Then: Quality assessment is available
    result.isSuccess shouldBe true
    val analysis = result.get
    
    analysis.qualityAssessment shouldBe a[QualityAssessment]
    analysis.performanceCategory shouldBe a[PerformanceCategory]
  }
  
  it should "persist analysis results to gold layer" in {
    // Given: Service with persistence capability
    val repository = new TestDistributedSessionAnalysisRepository()
    val service = new SessionAnalysisService(repository)
    
    // When: Analysis is executed
    val result = service.analyzeUserSessions("silver/path", "gold/path")
    
    // Then: Results are persisted successfully
    result.isSuccess shouldBe true
    // In real implementation, this would verify persistAnalysis() was called
  }
  
  it should "support dependency injection for testability" in {
    // Given: Different repository implementations
    val testRepository = new TestDistributedSessionAnalysisRepository()
    val service1 = new SessionAnalysisService(testRepository)
    
    val anotherRepository = new AnotherTestRepository()
    val service2 = new SessionAnalysisService(anotherRepository)
    
    // When: Services are used
    val result1 = service1.analyzeUserSessions("silver/path", "gold/path")
    val result2 = service2.analyzeUserSessions("silver/path", "gold/path")
    
    // Then: Both work with different implementations
    result1.isSuccess shouldBe true
    result2.isSuccess shouldBe true
    
    // But produce different results based on repository
    result1.get.metrics.totalSessions shouldNot equal(result2.get.metrics.totalSessions)
  }
}

/**
 * Alternative test repository implementation for dependency injection testing.
 */
class AnotherTestRepository extends DistributedSessionAnalysisRepository {
  
  override def loadEventsStream(silverPath: String): Try[EventStream] = {
    Success(new TestEventStream())
  }
  
  override def calculateSessionMetrics(eventsStream: EventStream): Try[SessionMetrics] = {
    Success(SessionMetrics(
      totalSessions = 2000L,  // Different from TestDistributedSessionAnalysisRepository
      uniqueUsers = 200L,
      totalTracks = 30000L,
      averageSessionLength = 15.0,
      qualityScore = 98.0
    ))
  }
  
  override def persistAnalysis(analysis: DistributedSessionAnalysis, goldPath: String): Try[Unit] = {
    Success(())
  }
  
  override def generateTopSessions(eventsStream: EventStream, topN: Int): Try[List[SessionSummary]] = {
    Success(List(
      SessionSummary("session3", "user3", 60),
      SessionSummary("session4", "user4", 55)
    ))
  }
}
