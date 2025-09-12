package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Try, Success, Failure}

/**
 * TDD-First specification for DistributedSessionAnalysisRepository interface.
 * 
 * Tests the repository abstraction following hexagonal architecture principles.
 * Focuses on contract verification without infrastructure dependencies.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DistributedSessionAnalysisRepositorySpec extends AnyFlatSpec with Matchers {

  "DistributedSessionAnalysisRepository" should "define contract for loading events as stream" in {
    // Given: Test implementation of repository
    val repository = new TestDistributedSessionAnalysisRepository()
    
    // When: Events are loaded
    val result = repository.loadEventsStream("silver/path")
    
    // Then: Stream is returned (contract verified)
    result.isSuccess shouldBe true
    result.get shouldBe a[TestEventStream]
  }
  
  it should "define contract for calculating session metrics" in {
    // Given: Repository and event stream
    val repository = new TestDistributedSessionAnalysisRepository()
    val eventStream = new TestEventStream()
    
    // When: Metrics are calculated
    val result = repository.calculateSessionMetrics(eventStream)
    
    // Then: Metrics are returned
    result.isSuccess shouldBe true
    result.get shouldBe a[SessionMetrics]
    result.get.totalSessions should be >= 0L
  }
  
  it should "define contract for persisting analysis results" in {
    // Given: Repository and analysis
    val repository = new TestDistributedSessionAnalysisRepository()
    val metrics = SessionMetrics(5000L, 200L, 75000L, 12.5, 97.0)
    val analysis = DistributedSessionAnalysis(metrics)
    
    // When: Analysis is persisted
    val result = repository.persistAnalysis(analysis, "gold/path")
    
    // Then: Persistence succeeds
    result.isSuccess shouldBe true
  }
  
  it should "handle repository failures gracefully" in {
    // Given: Repository that fails
    val repository = new TestDistributedSessionAnalysisRepository()
    
    // When: Operation fails with invalid path
    val result = repository.loadEventsStream("invalid/path")
    
    // Then: Failure is handled appropriately
    result.isFailure shouldBe true
    result.failed.get shouldBe a[RuntimeException]
  }
}

/**
 * Test implementation of DistributedSessionAnalysisRepository for TDD.
 */
class TestDistributedSessionAnalysisRepository extends DistributedSessionAnalysisRepository {
  
  override def loadEventsStream(silverPath: String): Try[EventStream] = {
    if (silverPath == "invalid/path") {
      Failure(new RuntimeException("Invalid path"))
    } else {
      Success(new TestEventStream())
    }
  }
  
  override def calculateSessionMetrics(eventsStream: EventStream): Try[SessionMetrics] = {
    Success(SessionMetrics(
      totalSessions = 1000L,
      uniqueUsers = 100L,
      totalTracks = 15000L,
      averageSessionLength = 15.0,
      qualityScore = 99.0
    ))
  }
  
  override def persistAnalysis(analysis: DistributedSessionAnalysis, goldPath: String): Try[Unit] = {
    Success(())
  }
  
  override def generateTopSessions(eventsStream: EventStream, topN: Int): Try[List[SessionSummary]] = {
    Success(List(
      SessionSummary("session1", "user1", 50),
      SessionSummary("session2", "user2", 45)
    ))
  }
}

/**
 * Test implementation of EventStream for TDD.
 */
class TestEventStream extends EventStream {
  
  override def partitionByUser(): EventStream = new TestEventStream()
  
  override def calculateSessions(sessionGap: java.time.Duration): SessionStream = new TestSessionStream()
  
  override def cache(): EventStream = this
  
  override def count(): Long = 1000L
  
  override def filter(predicate: ListenEvent => Boolean): EventStream = this
}

/**
 * Test implementation of SessionStream for TDD.
 */
class TestSessionStream extends SessionStream {
  
  override def aggregateMetrics(): Try[SessionMetrics] = {
    Success(SessionMetrics(
      totalSessions = 500L,
      uniqueUsers = 50L,
      totalTracks = 7500L,
      averageSessionLength = 15.0,
      qualityScore = 98.0
    ))
  }
  
  override def topSessions(n: Int): Try[List[SessionSummary]] = {
    Success(List(
      SessionSummary("session1", "user1", 50),
      SessionSummary("session2", "user2", 45)
    ).take(n))
  }
  
  override def persist(path: String): Try[Unit] = Success(())
  
  override def count(): Long = 500L
  
  override def filter(predicate: SessionSummary => Boolean): SessionStream = this
}
