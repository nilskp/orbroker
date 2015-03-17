package org.orbroker

import java.sql.ResultSet
import org.orbroker.adapt.ColumnNameAdapter
import java.sql.SQLException
import org.orbroker.exception.ConfigurationException
import java.sql.CallableStatement
import org.orbroker.exception.NullValueException

final class Column private[orbroker] (name: String, rs: ResultSet, tz: java.util.TimeZone, adapter: ColumnNameAdapter) {

  private def columns: Seq[String] = {
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

  private def columnIdx(column: String): Int = {
    if (column.length > 3) return 0
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

  private def newException(e: SQLException) = {
    val msg = s"""Could not find column "$name". Available columns are: ${columns.mkString(", ")}"""
    new ConfigurationException(msg, e)
  }

  private def getValue[T](extractor: ValueExtractor[T]): T = try columnIdx(name) match {
    case 0 => extractor(name, rs, tz)
    case idx => extractor(idx, rs, tz)
  } catch {
    case e: SQLException => throw newException(e)
  }

  def as[T](implicit extractor: ValueExtractor[T]): T = {
    val value = getValue(extractor)
    if (rs.wasNull) {
      throw new NullValueException(name)
    } else {
      value
    }
  }

  def opt[T](implicit extractor: ValueExtractor[T]): Option[T] = {
    val value = getValue(extractor)
    if (rs.wasNull) {
      None
    } else {
      Some(value)
    }
  }
}
