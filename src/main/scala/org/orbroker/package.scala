package org

import java.sql.ResultSet
import java.sql.CallableStatement
import language.implicitConversions
import org.joda.time.DateTimeZone
import org.orbroker.conv.UUIDBinaryConv
import org.orbroker.conv.LocaleConverter

/** Main package for O/R Broker. */
package object orbroker {

  import java.io.{ Reader, LineNumberReader }
  private[orbroker] implicit def readerToIterator(reader: Reader): Iterator[String] = {
    val lnr =
      if (reader.isInstanceOf[LineNumberReader])
        reader.asInstanceOf[LineNumberReader]
      else
        new LineNumberReader(reader)
    new Iterator[String] {
      var n: String = lnr.readLine
      override def hasNext = {
        n != null
      }
      override def next = {
        val nn = n
        n = lnr.readLine
        nn
      }
    }
  }

  private[orbroker] val NO_ID = Symbol("<noname>")

  implicit def stringToReader(string: String) = new java.io.StringReader(string)
  implicit def sym2Token[T](id: Symbol): Token[T] = Token(id)
  implicit def sql2Token[T](sql: String): Token[T] = Token(sql, NO_ID)

  import scala.collection.{ Traversable, TraversableView, Iterator }
  import scala.collection.generic.FilterMonadic

  implicit def viewToTraversable[A](view: TraversableView[A, Traversable[A]]) = new Traversable[A] {
    def foreach[U](callback: (A) => U) = view.foreach(callback)
  }

  implicit def filterMonadicToTraversable[A](monadic: FilterMonadic[A, Traversable[A]]) = new Traversable[A] {
    def foreach[U](callback: (A) => U) = monadic.foreach(callback)
  }

  implicit def iteratorToTraversable[A](iterator: Iterator[A]) = new Traversable[A] {
    def foreach[U](callback: (A) => U) {
      while (iterator.hasNext) callback(iterator.next)
    }
  }

  implicit def stringToDataSource(url: String) = new config.SimpleDataSource(url)

  implicit val IntValue = new ValueExtractor[Int] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone): Int = rs.getInt(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone): Int = rs.getInt(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone): Int = cs.getInt(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone): Int = cs.getInt(column)
  }
  implicit val LongExtractor = new ValueExtractor[Long] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone): Long = rs.getLong(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone): Long = rs.getLong(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone): Long = cs.getLong(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone): Long = cs.getLong(column)
  }
  implicit val DoubleValue = new ValueExtractor[Double] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone): Double = rs.getDouble(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone): Double = rs.getDouble(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone): Double = cs.getDouble(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone): Double = cs.getDouble(column)
  }
  implicit val FloatValue = new ValueExtractor[Float] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone): Float = rs.getFloat(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone): Float = rs.getFloat(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone): Float = cs.getFloat(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone): Float = cs.getFloat(column)
  }
  implicit val BooleanValue = new ValueExtractor[Boolean] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone): Boolean = rs.getBoolean(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone): Boolean = rs.getBoolean(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone): Boolean = cs.getBoolean(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone): Boolean = cs.getBoolean(column)
  }
  implicit val StringValue = new ValueExtractor[String] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone): String = rs.getString(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone): String = rs.getString(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone): String = cs.getString(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone): String = cs.getString(column)
  }
  implicit val ByteValue = new ValueExtractor[Byte] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getByte(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getByte(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getByte(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getByte(column)
  }
  implicit val ShortValue = new ValueExtractor[Short] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getShort(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getShort(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getShort(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getShort(column)
  }
  implicit val JBDValue = new ValueExtractor[java.math.BigDecimal] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getBigDecimal(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getBigDecimal(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getBigDecimal(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getBigDecimal(column)
  }
  implicit val BDValue = new ValueExtractor[BigDecimal] {
    private def convert(bd: java.math.BigDecimal) = bd match {
      case null => null
      case _ => BigDecimal(bd)
    }
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getBigDecimal(column))
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getBigDecimal(column))
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getBigDecimal(column))
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getBigDecimal(column))
  }
  implicit def ArrayColumn[T] = new ValueExtractor[Array[T]] {
    private def convert(arr: java.sql.Array) = arr match {
      case null => null
      case _ => arr.getArray.asInstanceOf[Array[T]]
    }
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getArray(column))
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getArray(column))
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getArray(column))
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getArray(column))
  }
  implicit val ByteArrayValue = new ValueExtractor[Array[Byte]] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getBytes(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getBytes(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getBytes(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getBytes(column)
  }
  implicit val DateValue = new ValueExtractor[java.sql.Date] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => rs.getDate(column)
      case _ => rs.getDate(column, tz)
    }
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => rs.getDate(column)
      case _ => rs.getDate(column, tz)
    }
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => cs.getDate(column)
      case _ => cs.getDate(column, tz)
    }
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => cs.getDate(column)
      case _ => cs.getDate(column, tz)
    }
  }
  implicit val TimeValue = new ValueExtractor[java.sql.Time] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => rs.getTime(column)
      case _ => rs.getTime(column, tz)
    }
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => rs.getTime(column)
      case _ => rs.getTime(column, tz)
    }
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => cs.getTime(column)
      case _ => cs.getTime(column, tz)
    }
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => cs.getTime(column)
      case _ => cs.getTime(column, tz)
    }
  }
  implicit val TimestampValue = new ValueExtractor[java.sql.Timestamp] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => rs.getTimestamp(column)
      case _ => rs.getTimestamp(column, tz)
    }
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => rs.getTimestamp(column)
      case _ => rs.getTimestamp(column, tz)
    }
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => cs.getTimestamp(column)
      case _ => cs.getTimestamp(column, tz)
    }
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => cs.getTimestamp(column)
      case _ => cs.getTimestamp(column, tz)
    }
  }
  implicit val AnyValue = new ValueExtractor[Any] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getObject(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getObject(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getObject(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getObject(column)
  }
  implicit val RefValue = new ValueExtractor[java.sql.Ref] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getRef(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getRef(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getRef(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getRef(column)
  }
  implicit val URLValue = new ValueExtractor[java.net.URL] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getURL(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getURL(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getURL(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getURL(column)
  }
  implicit val BlobValue = new ValueExtractor[java.sql.Blob] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getBlob(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getBlob(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getBlob(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getBlob(column)
  }
  implicit val ClobValue = new ValueExtractor[java.sql.Clob] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getClob(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getClob(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getClob(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getClob(column)
  }
  implicit val BinaryValue = new ValueExtractor[java.io.InputStream] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getBinaryStream(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getBinaryStream(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = ???
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = ???
  }
  val AsciiValue = new ValueExtractor[java.io.InputStream] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getAsciiStream(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getAsciiStream(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = ???
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = ???
  }
  implicit val CharStreamValue = new ValueExtractor[java.io.Reader] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getCharacterStream(column)
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getCharacterStream(column)
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getCharacterStream(column)
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getCharacterStream(column)
  }
  implicit def UtilDateValue = TimestampValue.asInstanceOf[ValueExtractor[java.util.Date]]

  implicit val JodaDateValue = new ValueExtractor[org.joda.time.LocalDate] {
    private def convert(date: java.sql.Date, tz: java.util.TimeZone): org.joda.time.LocalDate = date match {
      case null => null
      case _ => tz match {
        case null => new org.joda.time.LocalDate(date)
        case _ => new org.joda.time.LocalDate(date, DateTimeZone.forID(tz.getID))
      }
    }
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => convert(rs.getDate(column), tz)
      case _ => convert(rs.getDate(column, tz), tz)
    }
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => convert(rs.getDate(column), tz)
      case _ => convert(rs.getDate(column, tz), tz)
    }
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => convert(cs.getDate(column), tz)
      case _ => convert(cs.getDate(column, tz), tz)
    }
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => convert(cs.getDate(column), tz)
      case _ => convert(cs.getDate(column, tz), tz)
    }
  }
  implicit val JodaTimeValue = new ValueExtractor[org.joda.time.LocalTime] {
    private def convert(time: java.sql.Time, tz: java.util.TimeZone): org.joda.time.LocalTime = time match {
      case null => null
      case _ => tz match {
        case null => new org.joda.time.LocalTime(time)
        case _ => new org.joda.time.LocalTime(time, DateTimeZone.forID(tz.getID))
      }
    }
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => convert(rs.getTime(column), tz)
      case _ => convert(rs.getTime(column, tz), tz)
    }
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => convert(rs.getTime(column), tz)
      case _ => convert(rs.getTime(column, tz), tz)
    }
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => convert(cs.getTime(column), tz)
      case _ => convert(cs.getTime(column, tz), tz)
    }
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => convert(cs.getTime(column), tz)
      case _ => convert(cs.getTime(column, tz), tz)
    }
  }
  implicit val JodaDateTimeValue = new ValueExtractor[org.joda.time.DateTime] {
    private def convert(ts: java.sql.Timestamp, tz: java.util.TimeZone): org.joda.time.DateTime = ts match {
      case null => null
      case _ => tz match {
        case null => new org.joda.time.DateTime(ts)
        case _ => new org.joda.time.DateTime(ts, DateTimeZone.forID(tz.getID))
      }
    }
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => convert(rs.getTimestamp(column), tz)
      case _ => convert(rs.getTimestamp(column, tz), tz)
    }
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = tz match {
      case null => convert(rs.getTimestamp(column), tz)
      case _ => convert(rs.getTimestamp(column, tz), tz)
    }
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => convert(cs.getTimestamp(column), tz)
      case _ => convert(cs.getTimestamp(column, tz), tz)
    }
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = tz match {
      case null => convert(cs.getTimestamp(column), tz)
      case _ => convert(cs.getTimestamp(column, tz), tz)
    }
  }
  implicit val UUIDValue = new ValueExtractor[java.util.UUID] {
    private def convert(bytes: Array[Byte]) = bytes match {
      case null => null
      case _ => UUIDBinaryConv.fromBytes(bytes)
    }
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getBytes(column))
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getBytes(column))
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getBytes(column))
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getBytes(column))
  }
  implicit val ResultSetValue = new ValueExtractor[ResultSet] {
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = rs.getObject(column).asInstanceOf[ResultSet]
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = rs.getObject(column).asInstanceOf[ResultSet]
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = cs.getObject(column).asInstanceOf[ResultSet]
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = cs.getObject(column).asInstanceOf[ResultSet]
  }
  implicit val LocaleValue = new ValueExtractor[java.util.Locale] {
    private def convert(languageTag: String) = languageTag match {
      case null => null
      case _ => LocaleConverter.fromTag(languageTag)
    }
    def apply(column: String, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getString(column))
    def apply(column: Int, rs: ResultSet, tz: java.util.TimeZone) = convert(rs.getString(column))
    def apply(column: String, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getString(column))
    def apply(column: Int, cs: CallableStatement, tz: java.util.TimeZone) = convert(cs.getString(column))
  }

}