package org.orbroker.conv

import java.util.Locale

/**
 * [[scala.math.BigDecimal]]=>[[java.math.BigDecimal]].
 */
object BigDecimalConv extends ParmConverter {
  type T = BigDecimal
  val fromType = classOf[T]
  def toJdbcType(dec: T) = dec.bigDecimal
}

/**
 * [[java.util.Date]]=>[[java.sql.Time]].
 */
object UtilDateToTimeConv extends ParmConverter {
  type T = java.util.Date
  val fromType = classOf[T]
  def toJdbcType(time: T) = new java.sql.Time(time.getTime)
}

/**
 * [[java.util.Date]]=>[[java.sql.Timestamp]].
 */
object UtilDateToTimestampConv extends ParmConverter {
  type T = java.util.Date
  val fromType = classOf[T]
  def toJdbcType(time: T) = new java.sql.Timestamp(time.getTime)
}

/**
 * [[java.util.Date]]=>[[java.sql.Date]].
 */
object UtilDateToDateConv extends ParmConverter {
  type T = java.util.Date
  val fromType = classOf[T]
  def toJdbcType(date: T) = new java.sql.Date(date.getTime)
}

/**
 * [[org.joda.time.LocalDate]]=>[[java.sql.Date]].
 */
object JodaLocalDateConv extends ParmConverter {
  type T = org.joda.time.LocalDate
  val fromType = classOf[T]
  def toJdbcType(date: T) = new java.sql.Date(date.toDateTimeAtStartOfDay.getMillis)
}

/**
 * [[org.joda.time.LocalTime]]=>[[java.sql.Time]].
 */
object JodaLocalTimeConv extends ParmConverter {
  type T = org.joda.time.LocalTime
  val fromType = classOf[T]
  def toJdbcType(time: T) = new java.sql.Time(time.getMillisOfDay)
}

/**
 * [[org.joda.time.DateTime]]=>[[java.sql.Timestamp]].
 */
object JodaDateTimeConv extends ParmConverter {
  type T = org.joda.time.DateTime
  val fromType = classOf[T]
  def toJdbcType(ts: T) = new java.sql.Timestamp(ts.getMillis)
}

/**
 * [[java.util.UUID]]<=>[[scala.Array[Byte](16)]].
 */
object UUIDBinaryConv extends ParmConverter {
  import java.nio.ByteBuffer
  type T = java.util.UUID
  val fromType = classOf[T]
  def toJdbcType(uuid: T): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)
    bb.array
  }

  /**
   * Convert from byte array to UUID, inverse of [[toJdbcType(java.util.UUID)]].
   * For use in query extractors.
   */
  def fromBytes(bytes: Array[Byte]): T = {
    val bb = ByteBuffer.wrap(bytes)
    new java.util.UUID(bb.getLong, bb.getLong)
  }
}

/**
 * [[java.net.InetAddress]]<=>[[scala.Array[Byte](4)]].
 */
object Inet4AddrBinaryConv extends ParmConverter {
  type T = java.net.Inet4Address
  val fromType = classOf[T]
  def toJdbcType(addr: T): Array[Byte] = addr.getAddress
}

object LocaleConverter extends ParmConverter {
  type T = Locale
  val fromType = classOf[T]
  def toJdbcType(l: T): String = l.toLanguageTag
  def fromTag(languageTag: String) = Locale.forLanguageTag(languageTag)
}
