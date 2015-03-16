package org.orbroker

import java.sql.ResultSet
import org.orbroker.adapt.BrokerAdapter

private[orbroker] class RowIterator[T](rs: ResultSet, adapter: BrokerAdapter, extractor: Row => T) extends Iterator[T] {
  private[this] val row = new ResultSetRow(rs, adapter, Map.empty)
  private[this] var nextUnknown = true
  private[this] var hasMore = false
  def hasNext: Boolean = {
    if (nextUnknown) {
      hasMore = rs.next()
      nextUnknown = false
    }
    hasMore
  }
  def next = {
    if (hasNext) {
      nextUnknown = true
      extractor(row)
    } else {
      throw new NoSuchElementException(s"Result set has no more rows")
    }
  }
}
