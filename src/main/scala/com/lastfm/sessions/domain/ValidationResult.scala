package com.lastfm.sessions.domain

/**
 * Simple validation result for data quality validation.
 * 
 * Represents the outcome of validating a single field or record,
 * either successful with the validated value or failed with an error message.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
sealed trait ValidationResult[+A]

/**
 * Successful validation result containing the validated value.
 * 
 * @param value The successfully validated and potentially cleaned value
 */
case class Valid[A](value: A) extends ValidationResult[A]

/**
 * Failed validation result containing the error reason.
 * 
 * @param reason Human-readable explanation of why validation failed
 */
case class Invalid(reason: String) extends ValidationResult[Nothing]