package org.orbroker.adapt

import java.sql.ResultSetMetaData

trait ColumnNameAdapter {
  def columnName(colIdx: Int, md: ResultSetMetaData): String
}

trait DefaultColumnNameAdapter extends ColumnNameAdapter {
  def columnName(colIdx: Int, md: ResultSetMetaData) = md.getColumnLabel(colIdx)
}