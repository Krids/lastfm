package com.lastfm.sessions.infrastructure

import com.lastfm.sessions.domain.ListenEvent
import org.scalatest.propspec.AnyPropSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * Property-based test specification for data loading operations.
 * 
 * Each property test validates exactly one universal property,
 * ensuring focused validation across large input spaces.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DataLoadingProperties extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataLoading-Properties")
    .master("local[2]")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
  
  val dataRepository = new SparkDataRepository()
  
  // Simple generators for focused testing
  val validUserIdGen: Gen[String] = for {
    id <- Gen.choose(1, 999999)
  } yield f"user_$id%06d"
  
  val validTimestampGen: Gen[Instant] = for {
    epochSeconds <- Gen.choose(1104537600L, 1262304000L) // 2005-2009 range
  } yield Instant.ofEpochSecond(epochSeconds)
  
  val validArtistNameGen: Gen[String] = Gen.oneOf(
    Gen.alphaNumStr.map(s => if (s.isEmpty) "Artist" else s),
    Gen.const("坂本龍一"), // Japanese characters
    Gen.const("Sigur Rós") // Special characters
  )
  
  val validTrackNameGen: Gen[String] = Gen.oneOf(
    Gen.alphaNumStr.map(s => if (s.isEmpty) "Track" else s),
    Gen.const("Composition 0919 (Live_2009_4_15)"),
    Gen.const("Café del Mar")
  )
  
  val validListenEventGen: Gen[ListenEvent] = for {
    userId <- validUserIdGen
    timestamp <- validTimestampGen
    artistName <- validArtistNameGen
    trackName <- validTrackNameGen
  } yield ListenEvent(userId, timestamp, None, artistName, None, trackName)
  
  // Helper to create temporary files and clean them up
  var tempFiles: List[Path] = List.empty
  
  def withTempFile[T](events: List[ListenEvent])(test: Path => T): T = {
    val tempFile = Files.createTempFile("property_test", ".tsv")
    tempFiles = tempFile :: tempFiles
    
    try {
      val content = events.map(e => 
        s"${e.userId}\t${e.timestamp}\t\t${e.artistName}\t\t${e.trackName}"
      ).mkString("\n")
      Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8))
      test(tempFile)
    } finally {
      Files.deleteIfExists(tempFile)
      tempFiles = tempFiles.filterNot(_ == tempFile)
    }
  }
  
  /**
   * Property tests for data preservation - each property focuses on one preservation aspect.
   */
  property("loading preserves event count") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 100) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          // Property: Event count should be preserved
          result.isSuccess should be(true)
          result.get should have size events.size
        }
      }
    }
  }
  
  property("loading preserves userId set") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 50) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          val originalUserIds = events.map(_.userId).toSet
          val loadedUserIds = result.get.map(_.userId).toSet
          
          // Property: User ID set should be preserved
          result.isSuccess should be(true)
          originalUserIds should equal(loadedUserIds)
        }
      }
    }
  }
  
  property("loading preserves timestamp values") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 50) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          val originalTimestamps = events.map(_.timestamp).toSet
          val loadedTimestamps = result.get.map(_.timestamp).toSet
          
          // Property: Timestamp values should be preserved
          result.isSuccess should be(true)
          originalTimestamps should equal(loadedTimestamps)
        }
      }
    }
  }
  
  property("loading preserves artist names") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 50) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          val originalArtists = events.map(_.artistName).toSet
          val loadedArtists = result.get.map(_.artistName).toSet
          
          // Property: Artist names should be preserved
          result.isSuccess should be(true)
          originalArtists should equal(loadedArtists)
        }
      }
    }
  }
  
  property("loading preserves track names") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 50) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          val originalTracks = events.map(_.trackName).toSet
          val loadedTracks = result.get.map(_.trackName).toSet
          
          // Property: Track names should be preserved
          result.isSuccess should be(true)
          originalTracks should equal(loadedTracks)
        }
      }
    }
  }
  
  /**
   * Property tests for data integrity - each property focuses on one integrity aspect.
   */
  property("loading maintains per-user event counts") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 50) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          val originalCounts = events.groupBy(_.userId).view.mapValues(_.size).toMap
          val loadedCounts = result.get.groupBy(_.userId).view.mapValues(_.size).toMap
          
          // Property: Per-user event counts should be maintained
          result.isSuccess should be(true)
          originalCounts should equal(loadedCounts)
        }
      }
    }
  }
  
  property("loading produces valid timestamps") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 50) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          // Property: All loaded timestamps should be non-null and within valid range
          result.isSuccess should be(true)
          result.get.foreach { event =>
            event.timestamp should not be null
            event.timestamp.isAfter(Instant.parse("2005-01-01T00:00:00Z")) should be(true)
            event.timestamp.isBefore(Instant.parse("2010-01-01T00:00:00Z")) should be(true)
          }
        }
      }
    }
  }
  
  property("loading produces non-empty required fields") {
    forAll(Gen.listOf(validListenEventGen)) { events =>
      whenever(events.nonEmpty && events.size <= 50) {
        withTempFile(events) { tempFile =>
          val result = dataRepository.loadListenEvents(tempFile.toString)
          
          // Property: All loaded events should have non-empty required fields
          result.isSuccess should be(true)
          result.get.foreach { event =>
            event.userId should not be empty
            event.artistName should not be empty
            event.trackName should not be empty
          }
        }
      }
    }
  }
  
  /**
   * Property tests for Unicode handling - each property focuses on one character type.
   */
  property("loading preserves Japanese characters") {
    val japaneseEventGen = for {
      userId <- validUserIdGen
      timestamp <- validTimestampGen
      artistName <- Gen.const("坂本龍一")
      trackName <- Gen.const("Composition 日本")
    } yield ListenEvent(userId, timestamp, None, artistName, None, trackName)
    
    // Use Gen.listOfN to ensure non-empty lists of controlled size
    val japaneseEventListGen = for {
      size <- Gen.choose(1, 10) // Generate 1-10 events
      events <- Gen.listOfN(size, japaneseEventGen)
    } yield events
    
    forAll(japaneseEventListGen) { events =>
      withTempFile(events) { tempFile =>
        val result = dataRepository.loadListenEvents(tempFile.toString)
        
        // Property: Japanese characters should be preserved
        result.isSuccess should be(true)
        result.get should have size events.size
        result.get.foreach { event =>
          event.artistName should be("坂本龍一")
          event.trackName should be("Composition 日本")
        }
      }
    }
  }
  
  property("loading preserves special characters") {
    val specialEventGen = for {
      userId <- validUserIdGen
      timestamp <- validTimestampGen
      artistName <- Gen.const("Sigur Rós")
      trackName <- Gen.const("Café del Mar")
    } yield ListenEvent(userId, timestamp, None, artistName, None, trackName)
    
    // Use Gen.listOfN to ensure non-empty lists of controlled size
    val specialEventListGen = for {
      size <- Gen.choose(1, 10) // Generate 1-10 events
      events <- Gen.listOfN(size, specialEventGen)
    } yield events
    
    forAll(specialEventListGen) { events =>
      withTempFile(events) { tempFile =>
        val result = dataRepository.loadListenEvents(tempFile.toString)
        
        // Property: Special characters should be preserved
        result.isSuccess should be(true)
        result.get should have size events.size
        result.get.foreach { event =>
          event.artistName should be("Sigur Rós")
          event.trackName should be("Café del Mar")
        }
      }
    }
  }
  
  /**
   * Property tests for error resilience - each property focuses on one resilience aspect.
   */
  property("loading never returns null events") {
    // Use Gen.listOfN to ensure non-empty lists of controlled size
    val nonNullEventListGen = for {
      size <- Gen.choose(1, 15) // Generate 1-15 events
      events <- Gen.listOfN(size, validListenEventGen)
    } yield events
    
    forAll(nonNullEventListGen) { events =>
      withTempFile(events) { tempFile =>
        val result = dataRepository.loadListenEvents(tempFile.toString)
        
        // Property: Result should never contain null events
        result.isSuccess should be(true)
        result.get should have size events.size
        result.get should not contain null
      }
    }
  }
  
  property("loading handles empty lists gracefully") {
    forAll(Gen.const(List.empty[ListenEvent])) { events =>
      withTempFile(events) { tempFile =>
        val result = dataRepository.loadListenEvents(tempFile.toString)
        
        // Property: Empty input should produce empty output without error
        result.isSuccess should be(true)
        result.get should be(empty)
      }
    }
  }
}