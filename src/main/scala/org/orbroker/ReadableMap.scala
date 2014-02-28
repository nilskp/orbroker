package org.orbroker

private[orbroker] trait ReadableMap {
  import java.sql.{ Clob, Blob, Ref, Time, Date, Timestamp }
  import java.net.URL

  implicit protected def newCalendar(tz: java.util.TimeZone) = {
    val cal = java.util.Calendar.getInstance
    cal.setTimeZone(tz)
    cal
  }

  /**
   * Boolean value.
   */
  def bit(name: String): Option[Boolean]

  /**
   * Single byte integer.
   */
  def tinyInt(name: String): Option[Byte]
  /**
   * Two byte integer.
   */
  def smallInt(name: String): Option[Short]
  /**
   * Four byte integer.
   */
  def integer(name: String): Option[Int]
  /**
   * Eight byte integer.
   */
  def bigInt(name: String): Option[Long]

  /**
   * Real number with single precision.
   */
  def real(name: String): Option[Float]
  /**
   * Real number with double precision.
   */
  def realDouble(name: String): Option[Double]
  /**
   * Exact precision decimal number.
   */
  def decimal(name: String): Option[java.math.BigDecimal]
  /**
   * Character string, either fixed or
   * variable length.
   */
  def string(name: String): Option[String]
  /**
   * Array type.
   */
  def array[T](name: String): Option[Array[T]]
  /**
   * Fixed length binary data.
   */
  def binary(name: String): Option[Array[Byte]]
  /**
   * Calendar date.
   */
  def date(name: String): Option[Date]
  /**
   * Calendar date.
   */
  def date(name: String, tz: java.util.TimeZone): Option[Date]
  /**
   * Any type.
   */
  def any[T](name: String): Option[T]
  /**
   * Reference type.
   */
  def ref(name: String): Option[Ref]
  /**
   * Time of day.
   */
  def time(name: String): Option[Time]
  /**
   * Time of day.
   */
  def time(name: String, tz: java.util.TimeZone): Option[Time]
  /**
   * Time stamp.
   */
  def timestamp(name: String): Option[Timestamp]
  /**
   * Time stamp.
   */
  def timestamp(name: String, tz: java.util.TimeZone): Option[Timestamp]
  /**
   * Data link.
   */
  def dataLink(name: String): Option[URL]
  /**
   * Binary large object.
   */
  def blob(name: String): Option[Blob]
  /**
   * Character large object.
   */
  def clob(name: String): Option[Clob]

}