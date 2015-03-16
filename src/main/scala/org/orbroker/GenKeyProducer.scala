package org.orbroker

import JdbcCloser._

import exception._
import callback.ExecutionCallback
import adapt.BrokerAdapter
import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }

private[orbroker] trait GenKeyProducer {

  implicit protected def callback: ExecutionCallback
  protected def adapter: BrokerAdapter

  protected def handleGeneratedKeys[G](
    token: Token[G], genKeyHandler: G => Unit,
    rs: ResultSet, expectedRows: Int) {
    val extractor: RowExtractor[G] = token.extractor match {
      case re: RowExtractor[_] => re
      case qe => throw new ConfigurationException("Statement '%s' needs a %s, not: %s".format(token.id.name, classOf[RowExtractor[_]].getSimpleName, qe.getClass.getName))
    }
    try {
      val row = new ResultSetRow(rs, adapter, Map.empty)
      var rowCounter = 0
      while (rs.next) {
        rowCounter += 1
        val genKey = extractor.extract(row)
        genKeyHandler(genKey)
      }
      if (rowCounter < expectedRows)
        throw new UnsupportedJDBCOperationException("Only " + rowCounter + " row(s) of generated keys was returned. Expected " + expectedRows)
    } finally {
      rs.checkAndClose(token.id)
    }
  }
}
