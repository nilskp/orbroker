package org.orbroker

import adapt._
import exception._
import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }

private[orbroker] class ResultSetRow(rs: ResultSet, adapter: ColumnNameAdapter, columnAlias: Map[String, String]) extends Row {

  private def alias(column: String) = {
    val upperCol = column.toUpperCase
    columnAlias.getOrElse(upperCol, upperCol)
  }

  def apply(column: String)(implicit tz: java.util.TimeZone = null): Column = new Column(alias(column), rs, tz, adapter)

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
