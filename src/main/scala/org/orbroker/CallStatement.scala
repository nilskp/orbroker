package org.orbroker

import JdbcCloser._

private[orbroker] trait CallStatement extends StaticStatement with ResultSetProducer with GenKeyProducer {

  def call[OP, T, R](
    token: Token[T],
    session: Session,
    parms: Map[String, _],
    keyHandler: Option[(T) => Unit],
    receivers: Seq[Iterator[T] => R],
    outParmHandler: Option[(OutParms) => OP]): (Int, OP, Seq[R]) = {
    val parsed = statement(parms)
    val started = System.nanoTime
    callback.beforeExecute(token.id, parsed.sql)
    val cs = parsed.prepareCall(session.connection, receivers.size > 0)
    try {
      setFeatures(cs, session)
      val values = setParms(token, cs, parsed.parmDefs, parms)
      val hasResultSet = cs.execute()
      val rowsUpdated = cs.getUpdateCount
      val receiverResults = new collection.mutable.ArrayBuffer[R](64)
      if (hasResultSet) {
        val receiversIter: Iterator[Iterator[T] => R] = receivers.iterator
        var rs = cs.getResultSet
        while (rs != null && receiversIter.hasNext) {
          val receiver = receiversIter.next()
          receiverResults += mapResult(token.extractor, rs, receiver)
          rs = if (cs.getMoreResults) cs.getResultSet else null
        }
      }
      if (rowsUpdated > 0) keyHandler.foreach {
        handleGeneratedKeys(token, _, cs.getGeneratedKeys, rowsUpdated)
      }

      val outParm: OP = outParmHandler match {
        case Some(handler) => {
          val outParms = new OutParmsImpl(token.id, parsed.parmIdxMap, cs, callback, adapter)
          handler(outParms)
        }
        case None => null.asInstanceOf[OP]
      }
      callback.afterExecute(token.id, parsed.sql, values, diffTimeInMicros(started))
      (rowsUpdated, outParm, receiverResults)
    } finally {
      cs.checkAndClose(id)
    }
  }

}
