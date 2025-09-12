package com.lastfm.sessions.domain

import java.time.Duration
import scala.util.Try

/**
 * Repository port for distributed session analysis following hexagonal architecture.
 * 
 * Defines the contract for loading, processing, and persisting session analysis data
 * without infrastructure dependencies. Implementations must provide distributed
 * processing capabilities without loading large datasets into memory.
 * 
 * Key Principles:
 * - Infrastructure-agnostic interface (domain port)
 * - Memory-efficient operations (no driver-side data collection)
 * - Distributed processing contract (stream-based operations)
 * - Clean error handling with Try monads
 * - Single Responsibility: session analysis data operations only
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait DistributedSessionAnalysisRepository {
  
  /**
   * Loads listening events as a lazy, distributed stream.
   * 
   * Contract Requirements:
   * - MUST NOT materialize data in driver memory
   * - MUST return a lazy stream for distributed processing
   * - MUST handle file format validation
   * - MUST support various input formats (TSV, Parquet, etc.)
   * 
   * @param silverPath Path to cleaned Silver layer data
   * @return Try containing EventStream or failure
   * @throws IllegalArgumentException if silverPath is null or empty
   */
  def loadEventsStream(silverPath: String): Try[EventStream]
  
  /**
   * Calculates session metrics using distributed aggregations.
   * 
   * Contract Requirements:
   * - MUST use distributed aggregations (no collect operations)
   * - MUST apply 20-minute session gap algorithm
   * - MUST calculate all metrics in single pass for efficiency
   * - MUST handle empty datasets gracefully
   * 
   * @param eventsStream Distributed stream of listening events
   * @return Try containing SessionMetrics or failure
   */
  def calculateSessionMetrics(eventsStream: EventStream): Try[SessionMetrics]
  
  /**
   * Persists session analysis results using distributed writes.
   * 
   * Contract Requirements:
   * - MUST write data distributively (no driver bottlenecks)
   * - MUST generate multiple output formats (sessions, metrics, summaries)
   * - MUST follow medallion architecture (Gold layer structure)
   * - MUST be idempotent (safe to re-run)
   * 
   * @param analysis Session analysis results to persist
   * @param goldPath Base path for Gold layer output
   * @return Try indicating success or failure
   */
  def persistAnalysis(analysis: DistributedSessionAnalysis, goldPath: String): Try[Unit]
  
  /**
   * Generates top sessions ranking using distributed sorting.
   * 
   * Contract Requirements:
   * - MUST use distributed sorting (no driver-side operations)
   * - MUST handle tie-breaking consistently
   * - MUST support configurable ranking size
   * 
   * @param eventsStream Distributed stream of listening events
   * @param topN Number of top sessions to return
   * @return Try containing top sessions or failure
   */
  def generateTopSessions(eventsStream: EventStream, topN: Int): Try[List[SessionSummary]]
}

/**
 * Abstract stream interface for distributed event processing.
 * 
 * Represents a lazy, distributed collection of listening events that can be
 * processed without loading into driver memory. Implementations should leverage
 * the underlying distributed processing framework (e.g., Spark DataFrames).
 */
trait EventStream {
  
  /**
   * Partitions events by user ID for optimal session analysis performance.
   * 
   * Ensures that all events for a user are in the same partition, enabling
   * efficient session calculation without shuffling during groupBy operations.
   * 
   * @return Partitioned event stream
   */
  def partitionByUser(): EventStream
  
  /**
   * Calculates user sessions using the specified gap threshold.
   * 
   * Applies the session boundary algorithm distributively across all partitions.
   * 
   * @param sessionGap Maximum time gap within a session
   * @return Stream of calculated sessions
   */
  def calculateSessions(sessionGap: Duration): SessionStream
  
  /**
   * Caches the stream for multiple operations.
   * 
   * Enables efficient reuse of computed results across multiple operations.
   * 
   * @return Cached event stream
   */
  def cache(): EventStream
  
  /**
   * Counts total events in the stream using distributed operations.
   * 
   * @return Total number of events
   */
  def count(): Long
  
  /**
   * Applies filtering predicates distributively.
   * 
   * @param predicate Filtering condition
   * @return Filtered event stream
   */
  def filter(predicate: ListenEvent => Boolean): EventStream
}

/**
 * Abstract stream interface for distributed session processing.
 * 
 * Represents a lazy, distributed collection of user sessions that can be
 * aggregated and analyzed without driver memory constraints.
 */
trait SessionStream {
  
  /**
   * Aggregates session metrics using distributed operations.
   * 
   * Calculates all session statistics in a single distributed pass.
   * 
   * @return Try containing aggregated SessionMetrics
   */
  def aggregateMetrics(): Try[SessionMetrics]
  
  /**
   * Generates top N sessions by track count using distributed sorting.
   * 
   * @param n Number of top sessions to return
   * @return Try containing list of top sessions
   */
  def topSessions(n: Int): Try[List[SessionSummary]]
  
  /**
   * Persists session data using distributed writes.
   * 
   * @param path Output path for session data
   * @return Try indicating success or failure
   */
  def persist(path: String): Try[Unit]
  
  /**
   * Counts total sessions using distributed operations.
   * 
   * @return Total number of sessions
   */
  def count(): Long
  
  /**
   * Applies filtering to sessions distributively.
   * 
   * @param predicate Session filtering condition
   * @return Filtered session stream
   */
  def filter(predicate: SessionSummary => Boolean): SessionStream
}

/**
 * Session summary data structure for distributed processing.
 * 
 * Lightweight representation of a user session optimized for distributed operations.
 * Contains essential session information without full track details.
 * 
 * @param sessionId Unique session identifier
 * @param userId User who created the session
 * @param trackCount Number of tracks in the session
 * @param durationMinutes Session duration in minutes
 * @param startTime Session start timestamp
 * @param uniqueTracks Number of unique tracks in session
 */
case class SessionSummary(
  sessionId: String,
  userId: String,
  trackCount: Int,
  durationMinutes: Long = 0L,
  startTime: java.time.Instant = java.time.Instant.now(),
  uniqueTracks: Int = 0
) {
  require(sessionId != null && sessionId.nonEmpty, "sessionId cannot be null or empty")
  require(userId != null && userId.nonEmpty, "userId cannot be null or empty")
  require(trackCount >= 0, "trackCount must be non-negative")
  require(durationMinutes >= 0, "durationMinutes must be non-negative")
  require(uniqueTracks >= 0, "uniqueTracks must be non-negative")
}
