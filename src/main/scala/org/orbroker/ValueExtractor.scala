package org.orbroker

import java.sql.ResultSet
import java.sql.CallableStatement

trait ValueExtractor[T] {

  import language.implicitConversions

  implicit protected def newCalendar(tz: java.util.TimeZone) = {
    val cal = java.util.Calendar.getInstance
    cal.setTimeZone(tz)
    cal
  }

  /**
   * Extract value by column name. Should return `null` on NULL values.
   */
  @specialized
  def apply(column: String, rs: ResultSet, tz: java.util.TimeZone): T

  /**
   * Extract value by column index. Should return `null` on NULL values.
   */
  @specialized
  def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone): T

  /**
   * Extract value by parameter name. Should return `null` on NULL values.
   */
  @specialized
  def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone): T

  /**
   * Extract value by parameter index. Should return `null` on NULL values.
   */
  @specialized
  def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone): T
}