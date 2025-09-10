package com.lastfm.sessions.domain

import scala.util.Try

/**
 * Port interface for loading listening event data.
 * 
 * This interface defines the contract for data loading adapters,
 * allowing the domain layer to remain independent of specific
 * data sources (files, databases, APIs, etc.).
 */
trait DataRepositoryPort {
  /**
   * Loads listening events from the specified path.
   * 
   * @param path Path to the data source
   * @return Try containing either a list of listening events or an error
   */
  def loadListenEvents(path: String): Try[List[ListenEvent]]
}