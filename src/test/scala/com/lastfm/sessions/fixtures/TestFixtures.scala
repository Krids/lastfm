package com.lastfm.sessions.fixtures

import com.lastfm.sessions.domain._
import com.lastfm.sessions.common.{Constants, ConfigurableConstants}
import java.time.Instant
import scala.util.Random
import scala.io.Source

/**
 * Reusable test data fixtures for consistent testing.
 * 
 * Provides lazy-loaded, cacheable test data for comprehensive testing
 * of the LastFM session analysis pipeline.
 * 
 * Design Principles:
 * - Lazy loading for performance
 * - Immutable, cacheable fixtures
 * - Realistic data patterns
 * - Edge case coverage
 * - Self-documenting test scenarios
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object TestFixtures {
  
  /**
   * Fixture trait for lazy loading and caching test data.
   * 
   * @tparam T Type of data provided by fixture
   */
  trait Fixture[T] {
    def name: String
    def description: String
    protected def load(): T
    
    private lazy val data: T = load()
    def get: T = data
    
    /**
     * Forces reload of fixture data (useful for testing fixture behavior).
     */
    def reload(): T = load()
  }
  
  /**
   * User-related test fixtures.
   */
  object Users {
    
    val singleUser = new Fixture[String] {
      val name = "single-user"
      val description = "A single test user ID following LastFM format"
      def load() = "user_000001"
    }
    
    val multipleUsers = new Fixture[List[String]] {
      val name = "multiple-users"
      val description = "List of 10 test user IDs for multi-user scenarios"
      def load() = (1 to 10).map(i => f"user_${i}%06d").toList
    }
    
    val heavyUser = new Fixture[String] {
      val name = "heavy-user"
      val description = "User with suspicious activity pattern (>100k plays)"
      def load() = "user_999999"
    }
    
    val edgeCaseUsers = new Fixture[Map[String, String]] {
      val name = "edge-case-users"
      val description = "Collection of edge case user scenarios"
      def load() = Map(
        "minimum-id" -> "user_000000",
        "maximum-id" -> "user_999999",
        "typical-id" -> "user_500000"
      )
    }
  }
  
  /**
   * Session-related test fixtures.
   */
  object Sessions {
    
    val shortSession = new Fixture[List[ListenEvent]] {
      val name = "short-session"
      val description = "Session with 5 tracks within session gap"
      def load() = {
        val startTime = Instant.parse("2023-01-01T10:00:00Z")
        (0 until 5).map { i =>
          ListenEvent.minimal(
            userId = Users.singleUser.get,
            timestamp = startTime.plusSeconds(i * 180), // 3 minutes apart (within 20-min gap)
            artistName = s"Artist_$i",
            trackName = s"Track_$i"
          )
        }.toList
      }
    }
    
    val longSession = new Fixture[List[ListenEvent]] {
      val name = "long-session"
      val description = "Session with 100 tracks for performance testing"
      def load() = {
        val startTime = Instant.parse("2023-01-01T10:00:00Z")
        (0 until 100).map { i =>
          ListenEvent.minimal(
            userId = Users.singleUser.get,
            timestamp = startTime.plusSeconds(i * 60), // 1 minute apart
            artistName = s"Artist_${i % 10}", // Some repeated artists
            trackName = s"Track_$i"
          )
        }.toList
      }
    }
    
    val multipleSessions = new Fixture[List[ListenEvent]] {
      val name = "multiple-sessions"
      val description = "3 sessions with proper 20+ minute gaps between them"
      def load() = {
        val sessionGapMinutes = ConfigurableConstants.SessionAnalysis.SESSION_GAP_MINUTES.value + 10 // 30 minutes
        val session1Start = Instant.parse("2023-01-01T10:00:00Z")
        val session2Start = session1Start.plusSeconds(sessionGapMinutes * 60)
        val session3Start = session2Start.plusSeconds(sessionGapMinutes * 60)
        
        val session1 = (0 until 10).map { i =>
          ListenEvent.minimal(
            userId = Users.singleUser.get,
            timestamp = session1Start.plusSeconds(i * 60),
            artistName = "Artist1",
            trackName = s"Track1_$i"
          )
        }
        
        val session2 = (0 until 15).map { i =>
          ListenEvent.minimal(
            userId = Users.singleUser.get,
            timestamp = session2Start.plusSeconds(i * 60),
            artistName = "Artist2",
            trackName = s"Track2_$i"
          )
        }
        
        val session3 = (0 until 8).map { i =>
          ListenEvent.minimal(
            userId = Users.singleUser.get,
            timestamp = session3Start.plusSeconds(i * 60),
            artistName = "Artist3",
            trackName = s"Track3_$i"
          )
        }
        
        (session1 ++ session2 ++ session3).toList.sortBy(_.timestamp)
      }
    }
    
    val edgeCaseSessions = new Fixture[Map[String, List[ListenEvent]]] {
      val name = "edge-case-sessions"
      val description = "Collection of edge case session scenarios"
      def load() = Map(
        
        "midnight-crossing" -> {
          val midnight = Instant.parse("2023-01-01T23:55:00Z")
          (0 until 10).map { i =>
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = midnight.plusSeconds(i * 120), // 2 minutes apart, crosses midnight
              artistName = "Midnight Artist",
              trackName = s"Night Track $i"
            )
          }.toList
        },
        
        "identical-timestamps" -> {
          val sameTime = Instant.parse("2023-01-01T12:00:00Z")
          (0 until 5).map { i =>
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = sameTime, // All exactly same timestamp
              artistName = s"Artist_$i",
              trackName = s"Track_$i"
            )
          }.toList
        },
        
        "exact-boundary" -> {
          val start = Instant.parse("2023-01-01T10:00:00Z")
          val sessionGapSeconds = ConfigurableConstants.SessionAnalysis.SESSION_GAP_MINUTES.value * 60
          List(
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = start,
              artistName = "Artist",
              trackName = "Track1"
            ),
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = start.plusSeconds(sessionGapSeconds), // Exactly at boundary
              artistName = "Artist",
              trackName = "Track2"
            )
          )
        },
        
        "single-track-session" -> {
          List(
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = Instant.parse("2023-01-01T10:00:00Z"),
              artistName = "Single Artist",
              trackName = "Single Track"
            )
          )
        },
        
        "repeat-tracks" -> {
          val startTime = Instant.parse("2023-01-01T10:00:00Z")
          (0 until 5).map { i =>
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = startTime.plusSeconds(i * 180),
              artistName = "Repeat Artist", // Same artist/track repeated
              trackName = "Repeat Track"
            )
          }.toList
        },
        
        "unicode-content" -> {
          val startTime = Instant.parse("2023-01-01T10:00:00Z")
          List(
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = startTime,
              artistName = "坂本龍一", // Japanese characters
              trackName = "Merry Christmas Mr. Lawrence"
            ),
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = startTime.plusSeconds(240),
              artistName = "Sigur Rós", // Icelandic characters
              trackName = "Hoppípolla"
            ),
            ListenEvent.minimal(
              userId = Users.singleUser.get,
              timestamp = startTime.plusSeconds(480),
              artistName = "Мумий Тролль", // Cyrillic characters
              trackName = "Владивосток 2000"
            )
          )
        }
      )
    }
  }
  
  /**
   * Quality metrics test fixtures.
   */
  object QualityMetrics {
    
    val excellent = new Fixture[DataQualityMetrics] {
      val name = "excellent-quality"
      val description = "Quality metrics representing excellent data (99.9%+ quality)"
      def load() = DataQualityMetrics(
        totalRecords = 1000000,
        validRecords = 999999,
        rejectedRecords = 1,
        rejectionReasons = Map("invalid_timestamp" -> 1L),
        trackIdCoverage = 95.5,
        suspiciousUsers = 0
      )
    }
    
    val good = new Fixture[DataQualityMetrics] {
      val name = "good-quality"
      val description = "Quality metrics representing good data (95-99% quality)"
      def load() = DataQualityMetrics(
        totalRecords = 1000000,
        validRecords = 980000,
        rejectedRecords = 20000,
        rejectionReasons = Map(
          "invalid_timestamp" -> 15000L,
          "invalid_userId" -> 5000L
        ),
        trackIdCoverage = 88.0,
        suspiciousUsers = 5
      )
    }
    
    val poor = new Fixture[DataQualityMetrics] {
      val name = "poor-quality"
      val description = "Quality metrics representing poor data (<95% quality)"
      def load() = DataQualityMetrics(
        totalRecords = 1000000,
        validRecords = 850000,
        rejectedRecords = 150000,
        rejectionReasons = Map(
          "invalid_userId" -> 50000L,
          "invalid_timestamp" -> 75000L,
          "missing_track" -> 25000L
        ),
        trackIdCoverage = 45.0,
        suspiciousUsers = 150
      )
    }
    
    val minimal = new Fixture[DataQualityMetrics] {
      val name = "minimal-data"
      val description = "Quality metrics for minimal dataset (edge case)"
      def load() = DataQualityMetrics(
        totalRecords = 10,
        validRecords = 10,
        rejectedRecords = 0,
        rejectionReasons = Map.empty,
        trackIdCoverage = 80.0,
        suspiciousUsers = 0
      )
    }
  }
  
  /**
   * File data fixtures for testing I/O operations.
   */
  object FileData {
    
    val validTsv = new Fixture[String] {
      val name = "valid-tsv"
      val description = "Valid TSV data for testing parsing and processing"
      def load() = {
        """user_000001	2023-01-01T10:00:00Z	mbid-artist-1	Artist 1	mbid-track-1	Track 1
          |user_000001	2023-01-01T10:03:00Z	mbid-artist-2	Artist 2	mbid-track-2	Track 2
          |user_000002	2023-01-01T11:00:00Z	mbid-artist-3	Artist 3	mbid-track-3	Track 3
          |user_000002	2023-01-01T11:05:00Z	mbid-artist-1	Artist 1	mbid-track-4	Track 4""".stripMargin
      }
    }
    
    val malformedTsv = new Fixture[String] {
      val name = "malformed-tsv"
      val description = "TSV data with various quality issues for testing validation"
      def load() = {
        """	2023-01-01T10:00:00Z	mbid-artist-1	Artist 1	mbid-track-1	Track 1
          |user_000001	invalid-timestamp	mbid-artist-2	Artist 2	mbid-track-2	Track 2
          |user_000002	2023-01-01T11:00:00Z				
          |user_invalid	2023-01-01T12:00:00Z	mbid-artist-3	Artist 3	mbid-track-3	Track 3""".stripMargin
      }
    }
    
    val unicodeTsv = new Fixture[String] {
      val name = "unicode-tsv"
      val description = "TSV data with Unicode characters for internationalization testing"
      def load() = {
        """user_000001	2023-01-01T10:00:00Z	mbid-1	坂本龍一	track-1	Merry Christmas Mr. Lawrence
          |user_000001	2023-01-01T10:05:00Z	mbid-2	Sigur Rós	track-2	Hoppípolla
          |user_000002	2023-01-01T11:00:00Z	mbid-3	Мумий Тролль	track-3	Владивосток 2000
          |user_000002	2023-01-01T11:10:00Z	mbid-4	Björk	track-4	Jóga""".stripMargin
      }
    }
    
    val largeTsv = new Fixture[String] {
      val name = "large-tsv"
      val description = "Large TSV data for performance testing (1000 records)"
      def load() = {
        val baseTime = Instant.parse("2023-01-01T10:00:00Z")
        (0 until 1000).map { i =>
          val userId = f"user_${i % 100}%06d" // 100 different users
          val timestamp = baseTime.plusSeconds(i * 60) // 1 minute apart
          val artistName = s"Artist_${i % 50}" // 50 different artists
          val trackName = s"Track_$i"
          val trackId = s"track-mbid-$i"
          val artistId = s"artist-mbid-${i % 50}"
          
          s"$userId\t$timestamp\t$artistId\t$artistName\t$trackId\t$trackName"
        }.mkString("\n")
      }
    }
    
    val emptyTsv = new Fixture[String] {
      val name = "empty-tsv"
      val description = "Empty TSV data for edge case testing"
      def load() = ""
    }
    
    val headerOnlyTsv = new Fixture[String] {
      val name = "header-only-tsv"
      val description = "TSV with header but no data rows"
      def load() = "userId\ttimestamp\tartistId\tartistName\ttrackId\ttrackName"
    }
  }
  
  /**
   * Session analysis fixtures.
   */
  object SessionAnalysis {
    
    val basicAnalysis = new Fixture[DistributedSessionAnalysis] {
      val name = "basic-analysis"
      val description = "Basic session analysis with good metrics"
      def load() = {
        val metrics = SessionMetrics(
          totalSessions = 1000,
          uniqueUsers = 100,
          totalTracks = 25000,
          averageSessionLength = 25.0,
          qualityScore = 99.5
        )
        DistributedSessionAnalysis(metrics)
      }
    }
    
    val performanceAnalysis = new Fixture[DistributedSessionAnalysis] {
      val name = "performance-analysis"
      val description = "Session analysis focused on performance scenarios"
      def load() = {
        val metrics = SessionMetrics(
          totalSessions = 100000,
          uniqueUsers = 1000,
          totalTracks = 5000000,
          averageSessionLength = 50.0,
          qualityScore = 99.9
        )
        DistributedSessionAnalysis(metrics)
      }
    }
  }
  
  /**
   * Complex test scenarios combining multiple fixtures.
   */
  object Scenarios {
    
    /**
     * Multi-user, multi-session scenario for comprehensive testing.
     */
    def multiUserMultiSession: Map[String, List[ListenEvent]] = {
      Users.multipleUsers.get.map { userId =>
        userId -> Sessions.multipleSessions.get.map(_.copy(userId = userId))
      }.toMap
    }
    
    /**
     * Data quality progression scenario for testing quality improvements.
     */
    def dataQualityProgression: List[DataQualityMetrics] = {
      List(
        QualityMetrics.poor.get,
        QualityMetrics.poor.get.copy(validRecords = 900000, rejectedRecords = 100000),
        QualityMetrics.good.get,
        QualityMetrics.excellent.get
      )
    }
    
    /**
     * Performance scaling scenario with different data sizes.
     */
    def performanceScaling: Map[String, List[ListenEvent]] = {
      Map(
        "small" -> generateEvents(100),
        "medium" -> generateEvents(10000),
        "large" -> generateEvents(100000)
      )
    }
    
    /**
     * Edge case scenario covering various boundary conditions.
     */
    def edgeCaseBoundaries: Map[String, List[ListenEvent]] = {
      val exactGap = ConfigurableConstants.SessionAnalysis.SESSION_GAP_MINUTES.value * 60 // seconds
      val baseTime = Instant.parse("2023-01-01T10:00:00Z")
      
      Map(
        "within-gap" -> List(
          createEvent("user_000001", baseTime, "Artist", "Track1"),
          createEvent("user_000001", baseTime.plusSeconds(exactGap - 1), "Artist", "Track2") // 1 second under
        ),
        "over-gap" -> List(
          createEvent("user_000001", baseTime, "Artist", "Track1"),
          createEvent("user_000001", baseTime.plusSeconds(exactGap + 1), "Artist", "Track2") // 1 second over
        ),
        "exact-gap" -> List(
          createEvent("user_000001", baseTime, "Artist", "Track1"),
          createEvent("user_000001", baseTime.plusSeconds(exactGap), "Artist", "Track2") // Exactly at boundary
        )
      )
    }
    
    /**
     * Ranking scenario with deterministic tie-breaking.
     */
    def rankingScenario: Map[String, List[ListenEvent]] = {
      val baseTime = Instant.parse("2023-01-01T10:00:00Z")
      
      Map(
        // User with long session (30 tracks)
        "long-session-user" -> (0 until 30).map { i =>
          createEvent("user_000001", baseTime.plusSeconds(i * 60), s"Artist_$i", s"Track_$i")
        }.toList,
        
        // User with medium session (20 tracks)
        "medium-session-user" -> (0 until 20).map { i =>
          createEvent("user_000002", baseTime.plusSeconds(i * 60), s"Artist_$i", s"Track_$i")
        }.toList,
        
        // User with short session (5 tracks)
        "short-session-user" -> (0 until 5).map { i =>
          createEvent("user_000003", baseTime.plusSeconds(i * 60), s"Artist_$i", s"Track_$i")
        }.toList
      )
    }
  }
  
  /**
   * Test data generators for dynamic fixture creation.
   */
  object Generators {
    
    /**
     * Generates a user ID with specified index.
     */
    def generateUserId(index: Int = Random.nextInt(1000)): String = {
      f"${Constants.DataPatterns.USER_ID_PREFIX}${index}%06d"
    }
    
    /**
     * Generates a track name with specified index.
     */
    def generateTrackName(index: Int = Random.nextInt(1000)): String = {
      s"Track_$index"
    }
    
    /**
     * Generates an artist name with specified index.
     */
    def generateArtistName(index: Int = Random.nextInt(100)): String = {
      s"Artist_$index"
    }
    
    /**
     * Generates a valid MusicBrainz ID.
     */
    def generateMBID(): String = {
      java.util.UUID.randomUUID().toString
    }
    
    /**
     * Generates a listen event with specified parameters.
     */
    def generateListenEvent(
      userId: String = generateUserId(),
      timestamp: Instant = Instant.now(),
      artistName: String = generateArtistName(),
      trackName: String = generateTrackName(),
      trackId: Option[String] = Some(generateMBID())
    ): ListenEvent = {
      ListenEvent.minimal(
        userId = userId,
        timestamp = timestamp,
        artistName = artistName,
        trackName = trackName
      )
    }
    
    /**
     * Generates session events for a user within session gap.
     */
    def generateSessionEvents(
      userId: String,
      startTime: Instant,
      trackCount: Int,
      gapMinutes: Int = 5  // Within session gap
    ): List[ListenEvent] = {
      (0 until trackCount).map { i =>
        generateListenEvent(
          userId = userId,
          timestamp = startTime.plusSeconds(i * gapMinutes * 60),
          trackName = generateTrackName(i)
        )
      }.toList
    }
    
    /**
     * Generates multiple sessions for a user with proper gaps.
     */
    def generateMultipleSessions(
      userId: String,
      sessionCount: Int,
      tracksPerSession: Int,
      sessionGapMinutes: Int = ConfigurableConstants.SessionAnalysis.SESSION_GAP_MINUTES.value + 10
    ): List[ListenEvent] = {
      val startTime = Instant.parse("2023-01-01T10:00:00Z")
      
      (0 until sessionCount).flatMap { sessionIdx =>
        val sessionStart = startTime.plusSeconds(sessionIdx * sessionGapMinutes * 60)
        generateSessionEvents(userId, sessionStart, tracksPerSession)
      }.toList
    }
  }
  
  // Helper methods for fixture creation
  
  /**
   * Creates a listen event with specified parameters.
   */
  private def createEvent(userId: String, timestamp: Instant, artist: String, track: String): ListenEvent = {
    ListenEvent.minimal(
      userId = userId,
      timestamp = timestamp,
      artistName = artist,
      trackName = track
    )
  }
  
  /**
   * Generates a list of events for performance testing.
   */
  private def generateEvents(count: Int): List[ListenEvent] = {
    val baseTime = Instant.parse("2023-01-01T10:00:00Z")
    (0 until count).map { i =>
      Generators.generateListenEvent(
        userId = Generators.generateUserId(i % 100), // 100 different users
        timestamp = baseTime.plusSeconds(i * 60),
        artistName = Generators.generateArtistName(i % 50) // 50 different artists
      )
    }.toList
  }
  
  /**
   * Loads test data from resources directory.
   */
  def loadFromResource(resourcePath: String): String = {
    try {
      Source.fromResource(resourcePath).mkString
    } catch {
      case _: Exception => 
        throw new RuntimeException(s"Could not load test resource: $resourcePath")
    }
  }
  
  /**
   * Creates temporary test data files.
   */
  def createTempTestFile(content: String, suffix: String = ".tsv"): String = {
    val tempFile = java.nio.file.Files.createTempFile("lastfm-test-", suffix)
    java.nio.file.Files.write(tempFile, content.getBytes("UTF-8"))
    tempFile.toString
  }
  
  /**
   * Validates that fixtures are properly formed.
   */
  def validateFixtures(): List[String] = {
    val validationErrors = scala.collection.mutable.ListBuffer[String]()
    
    try {
      // Validate user fixtures
      val singleUser = Users.singleUser.get
      if (!singleUser.matches(Constants.DataPatterns.USER_ID_PATTERN)) {
        validationErrors += s"Invalid user ID in singleUser fixture: $singleUser"
      }
      
      // Validate session fixtures
      val shortSession = Sessions.shortSession.get
      if (shortSession.isEmpty) {
        validationErrors += "Short session fixture is empty"
      }
      
      // Validate quality fixtures
      val excellentQuality = QualityMetrics.excellent.get
      if (excellentQuality.qualityScore < 99.0) {
        validationErrors += s"Excellent quality fixture has low quality score: ${excellentQuality.qualityScore}"
      }
      
    } catch {
      case e: Exception =>
        validationErrors += s"Fixture validation failed: ${e.getMessage}"
    }
    
    validationErrors.toList
  }
}