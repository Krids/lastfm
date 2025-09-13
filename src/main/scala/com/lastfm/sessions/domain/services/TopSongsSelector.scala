package com.lastfm.sessions.domain.services

import com.lastfm.sessions.domain.TrackPopularity

/**
 * Domain service for selecting and ranking top songs.
 * 
 * Final selection of top N tracks based on popularity metrics.
 * Pure domain logic with no infrastructure dependencies.
 * 
 * Ranking criteria (in order):
 * 1. Play count (descending)
 * 2. Unique sessions (descending)
 * 3. Unique users (descending)
 * 4. Track name (lexicographic for stability)
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TopSongsSelector {
  
  /**
   * Selects top N songs by popularity.
   * 
   * @param tracks All track popularity data
   * @param n Number of top songs to select
   * @return Top N tracks with assigned ranks
   */
  def selectTopSongs(tracks: List[TrackPopularity], n: Int): List[TrackPopularity] = {
    if (n <= 0 || tracks.isEmpty) {
      List.empty
    } else {
      val sorted = tracks.sortWith(compareTracksForRanking)
      val topN = sorted.take(n)
      assignRanks(topN)
    }
  }
  
  /**
   * Comparison function for deterministic track ranking.
   * 
   * @param t1 First track
   * @param t2 Second track
   * @return true if t1 should rank higher than t2
   */
  private def compareTracksForRanking(t1: TrackPopularity, t2: TrackPopularity): Boolean = {
    // Primary: Play count (descending)
    val playComparison = t2.playCount.compareTo(t1.playCount)
    if (playComparison != 0) {
      return playComparison < 0
    }
    
    // Secondary: Unique sessions (descending)
    val sessionComparison = t2.uniqueSessions.compareTo(t1.uniqueSessions)
    if (sessionComparison != 0) {
      return sessionComparison < 0
    }
    
    // Tertiary: Unique users (descending)
    val userComparison = t2.uniqueUsers.compareTo(t1.uniqueUsers)
    if (userComparison != 0) {
      return userComparison < 0
    }
    
    // Quaternary: Track name (lexicographic for stability)
    t1.trackName.compareTo(t2.trackName) < 0
  }
  
  /**
   * Assigns sequential rank positions to sorted tracks.
   * 
   * @param sortedTracks Tracks already sorted by ranking criteria
   * @return Tracks with assigned rank positions
   */
  private def assignRanks(sortedTracks: List[TrackPopularity]): List[TrackPopularity] = {
    sortedTracks.zipWithIndex.map { case (track, index) =>
      track.copy(rank = index + 1)
    }
  }
  
  /**
   * Validates selection results meet business requirements.
   * 
   * @param selectedTracks Selected top tracks
   * @param requestedCount Number of tracks requested
   * @return Validation result with any issues found
   */
  def validateSelection(
    selectedTracks: List[TrackPopularity],
    requestedCount: Int
  ): SelectionValidation = {
    val issues = scala.collection.mutable.ListBuffer[String]()
    
    // Check count (may be less if insufficient data)
    if (selectedTracks.size > requestedCount) {
      issues += s"Too many tracks selected: ${selectedTracks.size} > $requestedCount"
    }
    
    // Check ranking sequence
    val ranks = selectedTracks.map(_.rank)
    if (ranks != (1 to selectedTracks.size).toList) {
      issues += s"Invalid rank sequence: $ranks"
    }
    
    // Check play count ordering
    val playCounts = selectedTracks.map(_.playCount)
    if (playCounts != playCounts.sorted.reverse) {
      issues += s"Play counts not properly ordered: $playCounts"
    }
    
    // Check for duplicates
    val uniqueKeys = selectedTracks.map(t => s"${t.artistName}|${t.trackName}").distinct
    if (uniqueKeys.size != selectedTracks.size) {
      issues += "Duplicate tracks detected in selection"
    }
    
    SelectionValidation(
      isValid = issues.isEmpty,
      issues = issues.toList
    )
  }
  
  /**
   * Calculates statistics about track selection.
   * 
   * @param tracks Tracks being analyzed
   * @return Statistics about the selection
   */
  def calculateSelectionStatistics(tracks: List[TrackPopularity]): SelectionStatistics = {
    if (tracks.isEmpty) {
      SelectionStatistics(0, 0, 0.0, 0, 0, 0.0, 0.0)
    } else {
      val playCounts = tracks.map(_.playCount)
      val sessionCounts = tracks.map(_.uniqueSessions)
      val userCounts = tracks.map(_.uniqueUsers)
      
      SelectionStatistics(
        totalTracks = tracks.size,
        totalPlayCount = playCounts.sum,
        averagePlayCount = playCounts.sum.toDouble / tracks.size,
        maxPlayCount = playCounts.max,
        minPlayCount = playCounts.min,
        averageSessionCount = sessionCounts.sum.toDouble / tracks.size,
        averageUserCount = userCounts.sum.toDouble / tracks.size
      )
    }
  }
  
  /**
   * Formats selected tracks for final output.
   * 
   * @param tracks Selected top tracks
   * @return Formatted output ready for presentation
   */
  def formatForOutput(tracks: List[TrackPopularity]): List[FormattedTrack] = {
    tracks.map { track =>
      FormattedTrack(
        rank = track.rank,
        trackName = track.trackName,
        artistName = track.artistName,
        playCount = track.playCount,
        formattedLine = s"${track.rank}\t${track.trackName}\t${track.artistName}\t${track.playCount}"
      )
    }
  }
}

/**
 * Result of selection validation.
 */
case class SelectionValidation(
  isValid: Boolean,
  issues: List[String]
)

/**
 * Statistics about track selection.
 */
case class SelectionStatistics(
  totalTracks: Int,
  totalPlayCount: Int,
  averagePlayCount: Double,
  maxPlayCount: Int,
  minPlayCount: Int,
  averageSessionCount: Double,
  averageUserCount: Double
)

/**
 * Track formatted for output.
 */
case class FormattedTrack(
  rank: Int,
  trackName: String,
  artistName: String,
  playCount: Int,
  formattedLine: String
)