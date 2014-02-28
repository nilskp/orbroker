package org.orbroker.callback

/**
 * Callback to serve as proxy for multiple callbacks.
 */
class MultiProxyCallback(delegates: ExecutionCallback*) extends ExecutionCallback {

  def beforeExecute(id: Symbol, sql: String) = delegates.foreach(_.beforeExecute(id, sql))

  def afterExecute(id: Symbol, sql: String, parms: Seq[Any], executionTime: Int) =
    delegates.foreach(_.afterExecute(id, sql, parms, executionTime))

  def beforeBatchExecute(id: Symbol, sql: String) = delegates.foreach(_.beforeBatchExecute(id, sql))

  def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int) =
    delegates.foreach(_.afterBatchExecute(id, sql, parms, executionTime))

  def onWarning(warning: java.sql.SQLWarning, id: Option[Symbol]) =
    delegates.foreach(_.onWarning(warning, id))

}
