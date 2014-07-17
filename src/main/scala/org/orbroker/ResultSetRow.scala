package org.orbroker

import adapt._
import exception._
import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }

private abstract class BasicResultSetRow(rs: ResultSet, adapter: ColumnNameAdapter) extends Row {

  protected def nullerr(column: String) = throw new IllegalStateException(s"Column $column is NULL")

  override def string(column: String) = rs.getString(column) match {
    case null => nullerr(column)
    case str => str
  }
  override def array[T](column: String) = rs.getArray(column) match {
    case null => nullerr(column)
    case arr => arr.getArray.asInstanceOf[Array[T]]
  }
  override def blob(column: String) = rs.getBlob(column) match {
    case null => nullerr(column)
    case blob => blob
  }
  override def clob(column: String) = rs.getClob(column) match {
    case null => nullerr(column)
    case clob => clob
  }
  override def binary(column: String) = rs.getBytes(column) match {
    case null => nullerr(column)
    case bin => bin
  }
  override def date(column: String, tz: java.util.TimeZone) = {
    val date = tz match {
      case null => rs.getDate(column)
      case tz => rs.getDate(column, tz)
    }
    date match {
      case null => nullerr(column)
      case date => date
    }
  }
  override def any[T](column: String) = rs.getObject(column) match {
    case null => nullerr(column)
    case obj => obj.asInstanceOf[T]
  }
  override def ref(column: String) = rs.getRef(column) match {
    case null => nullerr(column)
    case ref => ref
  }
  override def time(column: String, tz: java.util.TimeZone) = {
    val time = tz match {
      case null => rs.getTime(column)
      case tz => rs.getTime(column, tz)
    }
    time match {
      case null => nullerr(column)
      case time => time
    }
  }
  override def timestamp(column: String, tz: java.util.TimeZone) = {
    val ts = tz match {
      case null => rs.getTimestamp(column)
      case tz => rs.getTimestamp(column, tz)
    }
    ts match {
      case null => nullerr(column)
      case ts => ts
    }
  }
  override def dataLink(column: String) = rs.getURL(column) match {
    case null => nullerr(column)
    case link => link
  }
  override def asciiStream(column: String) = rs.getAsciiStream(column) match {
    case null => nullerr(column)
    case str => str
  }
  override def binaryStream(column: String) = rs.getBinaryStream(column) match {
    case null => nullerr(column)
    case str => str
  }
  override def charStream(column: String) = rs.getCharacterStream(column) match {
    case null => nullerr(column)
    case str => str
  }

  override def string_(column: String) = Option(rs.getString(column))
  override def array_[T](column: String) = Option(rs.getArray(column).getArray.asInstanceOf[Array[T]])
  override def blob_(column: String) = Option(rs.getBlob(column))
  override def clob_(column: String) = Option(rs.getClob(column))
  override def binary_(column: String) = Option(rs.getBytes(column))
  override def date_(column: String, tz: java.util.TimeZone) = Option(rs.getDate(column, tz))
  override def any_[T](column: String) = Option(rs.getObject(column).asInstanceOf[T])
  override def ref_(column: String) = Option(rs.getRef(column))
  override def time_(column: String, tz: java.util.TimeZone) = Option(rs.getTime(column, tz))
  override def timestamp_(column: String, tz: java.util.TimeZone) = Option(rs.getTimestamp(column, tz))
  override def dataLink_(column: String) = Option(rs.getURL(column))
  override def asciiStream_(column: String) = Option(rs.getAsciiStream(column))
  override def binaryStream_(column: String) = Option(rs.getBinaryStream(column))
  override def charStream_(column: String) = Option(rs.getCharacterStream(column))

  override def bit(column: String) = {
    val value = rs.getBoolean(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def bit_(column: String) = {
    val value = rs.getBoolean(column)
    if (rs.wasNull) None else Some(value)
  }
  override def tinyInt(column: String) = {
    val value = rs.getByte(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def tinyInt_(column: String) = {
    val value = rs.getByte(column)
    if (rs.wasNull) None else Some(value)
  }
  override def smallInt(column: String) = {
    val value = rs.getShort(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def smallInt_(column: String) = {
    val value = rs.getShort(column)
    if (rs.wasNull) None else Some(value)
  }
  override def integer(column: String) = {
    val value = rs.getInt(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def integer_(column: String) = {
    val value = rs.getInt(column)
    if (rs.wasNull) None else Some(value)
  }
  override def bigInt(column: String) = {
    val value = rs.getLong(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def bigInt_(column: String) = {
    val value = rs.getLong(column)
    if (rs.wasNull) None else Some(value)
  }
  override def real(column: String) = {
    val value = rs.getFloat(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def real_(column: String) = {
    val value = rs.getFloat(column)
    if (rs.wasNull) None else Some(value)
  }
  override def realDouble(column: String) = {
    val value = rs.getDouble(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def realDouble_(column: String) = {
    val value = rs.getDouble(column)
    if (rs.wasNull) None else Some(value)
  }
  override def decimal(column: String) = {
    val value = rs.getBigDecimal(column)
    if (rs.wasNull) nullerr(column) else value
  }
  override def decimal_(column: String) = {
    val value = rs.getBigDecimal(column)
    if (rs.wasNull) None else Some(value)
  }

  final lazy val columns: Seq[String] = {
    val metaData = rs.getMetaData
    val colCount = metaData.getColumnCount
    val seq = new Array[String](colCount)
    var i = 0
    while (i < colCount) {
      seq(i) = adapter.columnName(i + 1, metaData)
      i += 1
    }
    seq
  }
}

private abstract class ColumnAliasRow(rs: ResultSet, cna: ColumnNameAdapter, columnAlias: Map[String, String]) extends BasicResultSetRow(rs, cna) {

  protected def alias(column: String) = {
    val upperCol = column.toUpperCase
    columnAlias.getOrElse(upperCol, upperCol)
  }

  override def bit(column: String) = super.bit(alias(column))
  override def tinyInt(column: String) = super.tinyInt(alias(column))
  override def smallInt(column: String) = super.smallInt(alias(column))
  override def integer(column: String) = super.integer(alias(column))
  override def bigInt(column: String) = super.bigInt(alias(column))
  override def real(column: String) = super.real(alias(column))
  override def realDouble(column: String) = super.realDouble(alias(column))
  override def decimal(column: String) = super.decimal(alias(column))
  override def string(column: String) = super.string(alias(column))
  override def array[T](column: String) = super.array(alias(column))
  override def asciiStream(column: String) = super.asciiStream(alias(column))
  override def binaryStream(column: String) = super.binaryStream(alias(column))
  override def blob(column: String) = super.blob(alias(column))
  override def clob(column: String) = super.clob(alias(column))
  override def charStream(column: String) = super.charStream(alias(column))
  override def binary(column: String) = super.binary(alias(column))
  override def date(column: String, tz: java.util.TimeZone) = super.date(alias(column), tz)
  override def any[T](column: String): T = super.any(alias(column))
  override def ref(column: String) = super.ref(alias(column))
  override def time(column: String, tz: java.util.TimeZone) = super.time(alias(column), tz)
  override def timestamp(column: String, tz: java.util.TimeZone) = super.timestamp(alias(column), tz)
  override def dataLink(column: String) = super.dataLink(alias(column))

  override def bit_(column: String) = super.bit_(alias(column))
  override def tinyInt_(column: String) = super.tinyInt_(alias(column))
  override def smallInt_(column: String) = super.smallInt_(alias(column))
  override def integer_(column: String) = super.integer_(alias(column))
  override def bigInt_(column: String) = super.bigInt_(alias(column))
  override def real_(column: String) = super.real_(alias(column))
  override def realDouble_(column: String) = super.realDouble_(alias(column))
  override def decimal_(column: String) = super.decimal_(alias(column))
  override def string_(column: String) = super.string_(alias(column))
  override def array_[T](column: String) = super.array_(alias(column))
  override def asciiStream_(column: String) = super.asciiStream_(alias(column))
  override def binaryStream_(column: String) = super.binaryStream_(alias(column))
  override def blob_(column: String) = super.blob_(alias(column))
  override def clob_(column: String) = super.clob_(alias(column))
  override def charStream_(column: String) = super.charStream_(alias(column))
  override def binary_(column: String) = super.binary_(alias(column))
  override def date_(column: String, tz: java.util.TimeZone) = super.date_(alias(column), tz)
  override def any_[T](column: String): Option[T] = super.any_(alias(column))
  override def ref_(column: String) = super.ref_(alias(column))
  override def time_(column: String, tz: java.util.TimeZone) = super.time_(alias(column), tz)
  override def timestamp_(column: String, tz: java.util.TimeZone) = super.timestamp_(alias(column), tz)
  override def dataLink_(column: String) = super.dataLink_(alias(column))

}

private abstract class ColumnIndexedRow(rs: ResultSet, cna: ColumnNameAdapter, columnAlias: Map[String, String]) extends ColumnAliasRow(rs, cna, columnAlias) {

  private def columnIdx(column: String): Int = {
    if (column.length > 2) return 0
    var pos = 0
    var idx = 0
    while (pos < column.length) {
      val c = column.charAt(pos)
      if ('0' > c || c > '9') return 0
      idx = idx * 10 + (c - '0')
      pos += 1
    }
    idx
  }

  override def bit(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.bit(column)
    else {
      val value = rs.getBoolean(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def bit_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.bit_(column)
    else {
      val value = rs.getBoolean(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def tinyInt(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.tinyInt(column)
    else {
      val value = rs.getByte(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def tinyInt_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.tinyInt_(column)
    else {
      val value = rs.getByte(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def smallInt(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.smallInt(column)
    else {
      val value = rs.getShort(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def smallInt_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.smallInt_(column)
    else {
      val value = rs.getShort(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def integer(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.integer(column)
    else {
      val value = rs.getInt(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def integer_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.integer_(column)
    else {
      val value = rs.getInt(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def bigInt(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.bigInt(column)
    else {
      val value = rs.getLong(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def bigInt_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.bigInt_(column)
    else {
      val value = rs.getLong(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def real(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.real(column)
    else {
      val value = rs.getFloat(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def real_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.real_(column)
    else {
      val value = rs.getFloat(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def realDouble(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.realDouble(column)
    else {
      val value = rs.getDouble(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def realDouble_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.realDouble_(column)
    else {
      val value = rs.getDouble(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def decimal(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.decimal(column)
    else {
      rs.getBigDecimal(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def decimal_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.decimal_(column)
    else {
      Option(rs.getBigDecimal(colIdx))
    }
  }
  override def string(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.string(column)
    else {
      rs.getString(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def string_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.string_(column)
    else {
      Option(rs.getString(colIdx))
    }
  }
  override def array[T](column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.array(column)
    else {
      val value = rs.getArray(colIdx)
      if (rs.wasNull) nullerr(column) else value.getArray.asInstanceOf[Array[T]]
    }
  }
  override def array_[T](column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.array_(column)
    else {
      val value = rs.getArray(colIdx)
      if (rs.wasNull) None else Option(value.getArray.asInstanceOf[Array[T]])
    }
  }
  override def asciiStream(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0) {
      super.asciiStream(column)
    } else {
      rs.getAsciiStream(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def asciiStream_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0) {
      super.asciiStream_(column)
    } else {
      Option(rs.getAsciiStream(colIdx))
    }
  }
  override def binaryStream(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.binaryStream(column)
    else {
      rs.getBinaryStream(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def binaryStream_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.binaryStream_(column)
    else {
      Option(rs.getBinaryStream(colIdx))
    }
  }
  override def blob(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0) {
      super.blob(column)
    } else {
      rs.getBlob(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def blob_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0) {
      super.blob_(column)
    } else {
      Option(rs.getBlob(colIdx))
    }
  }
  override def clob(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.clob(column)
    else {
      rs.getClob(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def clob_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.clob_(column)
    else {
      Option(rs.getClob(colIdx))
    }
  }
  override def charStream(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.charStream(column)
    else {
      rs.getCharacterStream(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def charStream_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.charStream_(column)
    else {
      Option(rs.getCharacterStream(colIdx))
    }
  }
  override def binary(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.binary(column)
    else {
      rs.getBytes(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def binary_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.binary_(column)
    else {
      Option(rs.getBytes(colIdx))
    }
  }
  override def date(column: String, tz: java.util.TimeZone) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.date(column)
    else {
      val value = tz match {
        case null => rs.getDate(colIdx)
        case tz => rs.getDate(colIdx, tz)
      }
      if (value == null) nullerr(column) else value
    }
  }
  override def date_(column: String, tz: java.util.TimeZone) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.date_(column, tz)
    else {
      tz match {
        case null => Option(rs.getDate(colIdx))
        case tz => Option(rs.getDate(colIdx, tz))
      }
    }
  }
  override def any[T](column: String): T = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.any[T](column)
    else {
      rs.getObject(colIdx) match {
        case null => nullerr(column)
        case value => value.asInstanceOf[T]
      }
    }
  }
  override def any_[T](column: String): Option[T] = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.any_(column)
    else {
      Option(rs.getObject(colIdx).asInstanceOf[T])
    }
  }
  override def ref(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.ref(column)
    else {
      val value = rs.getRef(colIdx)
      if (rs.wasNull) nullerr(column) else value
    }
  }
  override def ref_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.ref_(column)
    else {
      val value = rs.getRef(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def time(column: String, tz: java.util.TimeZone) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.time(column)
    else {
      val value = tz match {
        case null => rs.getTime(colIdx)
        case tz => rs.getTime(colIdx, tz)
      }
      if (value == null) nullerr(column) else value
    }
  }
  override def time_(column: String, tz: java.util.TimeZone) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.time_(column)
    else {
      val value = tz match {
        case null => rs.getTime(colIdx)
        case tz => rs.getTime(colIdx, tz)
      }
      Option(value)
    }
  }
  override def timestamp(column: String, tz: java.util.TimeZone) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.timestamp(column)
    else {
      val value = tz match {
        case null => rs.getTimestamp(colIdx)
        case tz => rs.getTimestamp(colIdx, tz)
      }
      if (value == null) nullerr(column) else value
    }
  }
  override def timestamp_(column: String, tz: java.util.TimeZone) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.timestamp_(column)
    else {
      val value = tz match {
        case null => rs.getTimestamp(colIdx)
        case tz => rs.getTimestamp(colIdx, tz)
      }
      Option(value)
    }
  }
  override def dataLink(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.dataLink(column)
    else {
      rs.getURL(colIdx) match {
        case null => nullerr(column)
        case value => value
      }
    }
  }
  override def dataLink_(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.dataLink_(column)
    else {
      Option(rs.getURL(colIdx))
    }
  }

}

private[orbroker] class ResultSetRow(rs: ResultSet, cna: ColumnNameAdapter, columnAlias: Map[String, String]) extends ColumnIndexedRow(rs, cna, columnAlias) {

  private def newException(column: String, e: SQLException) = {
    val msg = "Could not find column \"" + alias(column) + "\". Available columns are: " + columns.mkString(", ")
    new ConfigurationException(msg, e)
  }

  override def bit(column: String) = try {
    super.bit(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def tinyInt(column: String) = try {
    super.tinyInt(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def smallInt(column: String) = try {
    super.smallInt(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def integer(column: String) = try {
    super.integer(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }

  override def bigInt(column: String) = try {
    super.bigInt(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def real(column: String) = try {
    super.real(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def realDouble(column: String) = try {
    super.realDouble(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def decimal(column: String) = try {
    super.decimal(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def string(column: String) = try {
    super.string(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def array[T](column: String) = try {
    super.array(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def asciiStream(column: String) = try {
    super.asciiStream(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def binaryStream(column: String) = try {
    super.binaryStream(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def blob(column: String) = try {
    super.blob(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def clob(column: String) = try {
    super.clob(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def charStream(column: String) = try {
    super.charStream(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def binary(column: String) = try {
    super.binary(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def any[T](column: String) = try {
    super.any(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def ref(column: String) = try {
    super.ref(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def dataLink(column: String) = try {
    super.dataLink(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def date(column: String, tz: java.util.TimeZone) = try {
    super.date(column, tz)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def time(column: String, tz: java.util.TimeZone) = try {
    super.time(column, tz)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def timestamp(column: String, tz: java.util.TimeZone) = try {
    super.timestamp(column, tz)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }

  override def bit_(column: String) = try {
    super.bit_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def tinyInt_(column: String) = try {
    super.tinyInt_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def smallInt_(column: String) = try {
    super.smallInt_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def integer_(column: String) = try {
    super.integer_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }

  override def bigInt_(column: String) = try {
    super.bigInt_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def real_(column: String) = try {
    super.real_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def realDouble_(column: String) = try {
    super.realDouble_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def decimal_(column: String) = try {
    super.decimal_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def string_(column: String) = try {
    super.string_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def array_[T](column: String) = try {
    super.array_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def asciiStream_(column: String) = try {
    super.asciiStream_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def binaryStream_(column: String) = try {
    super.binaryStream_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def blob_(column: String) = try {
    super.blob_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def clob_(column: String) = try {
    super.clob_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def charStream_(column: String) = try {
    super.charStream_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def binary_(column: String) = try {
    super.binary_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def any_[T](column: String) = try {
    super.any_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def ref_(column: String) = try {
    super.ref_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def dataLink_(column: String) = try {
    super.dataLink_(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def date_(column: String, tz: java.util.TimeZone) = try {
    super.date_(column, tz)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def time_(column: String, tz: java.util.TimeZone) = try {
    super.time_(column, tz)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def timestamp_(column: String, tz: java.util.TimeZone) = try {
    super.timestamp_(column, tz)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }

}
