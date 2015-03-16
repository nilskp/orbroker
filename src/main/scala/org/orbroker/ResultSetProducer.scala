package org.orbroker

import JdbcCloser._
import exception._
import callback.ExecutionCallback
import adapt.BrokerAdapter
import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }

private[orbroker] trait ResultSetProducer {
  private[orbroker] val id: Symbol
  implicit protected val callback: ExecutionCallback
  protected val adapter: BrokerAdapter

  protected def setFeatures(stm: java.sql.Statement, session: Session) {
    if (stm.getFetchSize != session.fetchSize) {
      stm.setFetchSize(session.fetchSize)
    }
    if (stm.getQueryTimeout != session.timeout) {
      stm.setQueryTimeout(session.timeout)
    }
  }

  private[orbroker] def mapResult[T, R](extractor: QueryExtractor[T], rs: ResultSet, receiver: Iterator[T] => R): R =
    try {
      extractor.mapResultSet(rs, receiver, adapter)
    } finally {
      rs.checkAndClose(id)
    }

}

