package org.orbroker

import java.sql.ResultSet
import org.orbroker.adapt.BrokerAdapter

private[orbroker] class JoinIterable[T](key: Set[String], rs: ResultSet, adapter: BrokerAdapter, extractor: (Row, Join) ⇒ T) {
  def iterator(): Iterator[T] =
    if (rs.next) {
      new JoinIterator(rs, adapter)
    } else {
      Iterator.empty
    }
  private class JoinIterator(rs: ResultSet, adapter: BrokerAdapter) extends Iterator[T] {
    private[this] var first = true
    private[this] val join = new JoinGroup(key, rs, Map.empty, adapter)
    def hasNext = first || join.rsReadable
    def next = {
      if (hasNext) {
        first = false
        join.newGroup()
        val value = extractor(join.row, join)
        if (!join.isRsAdvanced) join.rsReadable = rs.next
        value
      } else {
        throw new NoSuchElementException(s"Result set has no more rows")
      }
    }

  }

}
