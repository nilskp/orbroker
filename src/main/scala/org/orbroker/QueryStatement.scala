package org.orbroker

import JdbcCloser._

private[orbroker] trait QueryStatement extends SQLStatement with ResultSetProducer {

  private def preparedAndQuery[T, R](
    token: Token[T], session: Session, parsed: ParsedSQL,
    parms: Map[String, Any], receiver: Iterator[T] => R): (Seq[Any], R) = {
    val ps = parsed.prepareQuery(session.connection)
    try {
      setFeatures(ps, session)
      val values = setParms(token, ps, parsed.parmDefs, parms)
      val rs = ps.executeQuery()
      values -> mapResult(token.extractor, rs, receiver)
    } finally {
      ps.checkAndClose(id)
    }
  }

  private def unpreparedAndQuery[T, R](
    token: Token[T], session: Session,
    parsed: ParsedSQL, receiver: Iterator[T] => R): R = {
    val stm = parsed.createStatement(session.connection)
    try {
      setFeatures(stm, session)
      val rs = stm.executeQuery(parsed.sql)
      mapResult(token.extractor, rs, receiver)
    } finally {
      stm.checkAndClose(id)
    }
  }

  def query[T, R](
    token: Token[T], session: Session,
    parms: Map[String, Any], receiver: Iterator[T] => R): R = {
    val started = System.nanoTime
    val parsed = statement(parms)
    callback.beforeExecute(token.id, parsed.sql)
    val (values, result) = if (!session.alwaysPrepare && parsed.parmDefs.isEmpty) {
      List.empty -> unpreparedAndQuery(token, session, parsed, receiver)
    } else {
      preparedAndQuery(token, session, parsed, parms, receiver)
    }
    callback.afterExecute(token.id, parsed.sql, values, diffTimeInMicros(started))
    result
  }

}
