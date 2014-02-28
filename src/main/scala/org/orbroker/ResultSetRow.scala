package org.orbroker

import adapt._
import exception._
import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }

private abstract class BasicResultSetRow(rs: ResultSet, adapter: ColumnNameAdapter) extends Row {

  override def string(column: String) = Option(rs.getString(column))
  override def array[T](column: String) = Option(rs.getArray(column).getArray.asInstanceOf[Array[T]])
  override def blob(column: String) = Option(rs.getBlob(column))
  override def clob(column: String) = Option(rs.getClob(column))
  override def binary(column: String) = Option(rs.getBytes(column))
  override def date(column: String) = Option(rs.getDate(column))
  override def date(column: String, tz: java.util.TimeZone) = Option(rs.getDate(column, tz))
  override def any[T](column: String) = Option(rs.getObject(column).asInstanceOf[T])
  override def ref(column: String) = Option(rs.getRef(column))
  override def time(column: String) = Option(rs.getTime(column))
  override def time(column: String, tz: java.util.TimeZone) = Option(rs.getTime(column, tz))
  override def timestamp(column: String) = Option(rs.getTimestamp(column))
  override def timestamp(column: String, tz: java.util.TimeZone) = Option(rs.getTimestamp(column, tz))
  override def dataLink(column: String) = Option(rs.getURL(column))
  override def asciiStream(column: String) = Option(rs.getAsciiStream(column))
  override def binaryStream(column: String) = Option(rs.getBinaryStream(column))
  override def charStream(column: String) = Option(rs.getCharacterStream(column))

  override def bit(column: String) = {
    val value = rs.getBoolean(column)
    if (rs.wasNull) None else Some(value)
  }
  override def tinyInt(column: String) = {
    val value = rs.getByte(column)
    if (rs.wasNull) None else Some(value)
  }
  override def smallInt(column: String) = {
    val value = rs.getShort(column)
    if (rs.wasNull) None else Some(value)
  }
  override def integer(column: String) = {
    val value = rs.getInt(column)
    if (rs.wasNull) None else Some(value)
  }
  override def bigInt(column: String) = {
    val value = rs.getLong(column)
    if (rs.wasNull) None else Some(value)
  }
  override def real(column: String) = {
    val value = rs.getFloat(column)
    if (rs.wasNull) None else Some(value)
  }
  override def realDouble(column: String) = {
    val value = rs.getDouble(column)
    if (rs.wasNull) None else Some(value)
  }
  override def decimal(column: String) = {
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
  override def date(column: String) = super.date(alias(column))
  override def any[T](column: String): Option[T] = super.any(alias(column))
  override def ref(column: String) = super.ref(alias(column))
  override def time(column: String) = super.time(alias(column))
  override def timestamp(column: String) = super.timestamp(alias(column))
  override def dataLink(column: String) = super.dataLink(alias(column))

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
      if (rs.wasNull) None else Some(value)
    }
  }
  override def tinyInt(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.tinyInt(column)
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
      if (rs.wasNull) None else Some(value)
    }
  }
  override def integer(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.integer(column)
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
      if (rs.wasNull) None else Some(value)
    }
  }
  override def real(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.real(column)
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
      if (rs.wasNull) None else Some(value)
    }
  }
  override def decimal(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.decimal(column)
    else {
      Option(rs.getBigDecimal(colIdx))
    }
  }
  override def string(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.string(column)
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
      if (rs.wasNull) None else Some(value.getArray.asInstanceOf[Array[T]])
    }
  }
  override def asciiStream(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0) {
      super.asciiStream(column)
    } else {
      Option(rs.getAsciiStream(colIdx))
    }
  }
  override def binaryStream(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.binaryStream(column)
    else {
      Option(rs.getBinaryStream(colIdx))
    }
  }
  override def blob(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0) {
      super.blob(column)
    } else {
      Option(rs.getBlob(colIdx))
    }
  }
  override def clob(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.clob(column)
    else {
      Option(rs.getClob(colIdx))
    }
  }
  override def charStream(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.charStream(column)
    else {
      Option(rs.getCharacterStream(colIdx))
    }
  }
  override def binary(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.binary(column)
    else {
      Option(rs.getBytes(colIdx))
    }
  }
  override def date(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.date(column)
    else {
      Option(rs.getDate(colIdx))
    }
  }
  override def any[T](column: String): Option[T] = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.any(column)
    else {
      Option(rs.getObject(colIdx)).asInstanceOf[Option[T]]
    }
  }
  override def ref(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.ref(column)
    else {
      val value = rs.getRef(colIdx)
      if (rs.wasNull) None else Some(value)
    }
  }
  override def time(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.time(column)
    else {
      Option(rs.getTime(colIdx))
    }
  }
  override def timestamp(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.timestamp(column)
    else {
      Option(rs.getTimestamp(colIdx))
    }
  }
  override def dataLink(column: String) = {
    val colIdx = columnIdx(column)
    if (colIdx == 0)
      super.dataLink(column)
    else {
      Option(rs.getURL(colIdx))
    }
  }

}

private[orbroker]class ResultSetRow(rs: ResultSet, cna: ColumnNameAdapter, columnAlias: Map[String, String]) extends ColumnIndexedRow(rs, cna, columnAlias) {

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
  override def date(column: String) = try {
    super.date(column)
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
  override def time(column: String) = try {
    super.time(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def timestamp(column: String) = try {
    super.timestamp(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }
  override def dataLink(column: String) = try {
    super.dataLink(column)
  } catch {
    case e: SQLException ⇒ throw newException(column, e)
  }

}
