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
  def bit(name: String): Boolean
  def bit_(name: String): Option[Boolean]

  /**
   * Single byte integer.
   */
  def tinyInt(name: String): Byte
  def tinyInt_(name: String): Option[Byte]
  /**
   * Two byte integer.
   */
  def smallInt(name: String): Short
  def smallInt_(name: String): Option[Short]
  /**
   * Four byte integer.
   */
  def integer(name: String): Int
  def integer_(name: String): Option[Int]
  /**
   * Eight byte integer.
   */
  def bigInt(name: String): Long
  def bigInt_(name: String): Option[Long]

  /**
   * Real number with single precision.
   */
  def real(name: String): Float
  def real_(name: String): Option[Float]
  /**
   * Real number with double precision.
   */
  def realDouble(name: String): Double
  def realDouble_(name: String): Option[Double]
  /**
   * Exact precision decimal number.
   */
  def decimal(name: String): java.math.BigDecimal
  def decimal_(name: String): Option[java.math.BigDecimal]
  /**
   * Character string, either fixed or
   * variable length.
   */
  def string(name: String): String
  def string_(name: String): Option[String]
  /**
   * Array type.
   */
  def array[T](name: String): Array[T]
  def array_[T](name: String): Option[Array[T]]
  /**
   * Fixed length binary data.
   */
  def binary(name: String): Array[Byte]
  def binary_(name: String): Option[Array[Byte]]
  /**
   * Calendar date.
   */
  def date(name: String, tz: java.util.TimeZone = null): Date
  def date_(name: String, tz: java.util.TimeZone = null): Option[Date]
  /**
   * Any type.
   */
  def any[T](name: String): T
  def any_[T](name: String): Option[T]
  /**
   * Reference type.
   */
  def ref(name: String): Ref
  def ref_(name: String): Option[Ref]
  /**
   * Time of day.
   */
  def time(name: String, tz: java.util.TimeZone = null): Time
  def time_(name: String, tz: java.util.TimeZone = null): Option[Time]
  /**
   * Time stamp.
   */
  def timestamp(name: String, tz: java.util.TimeZone = null): Timestamp
  def timestamp_(name: String, tz: java.util.TimeZone = null): Option[Timestamp]
  /**
   * Data link.
   */
  def dataLink(name: String): URL
  def dataLink_(name: String): Option[URL]
  /**
   * Binary large object.
   */
  def blob(name: String): Blob
  def blob_(name: String): Option[Blob]
  /**
   * Character large object.
   */
  def clob(name: String): Clob
  def clob_(name: String): Option[Clob]

}