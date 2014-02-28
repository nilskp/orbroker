package org.orbroker.callback

import java.sql.Connection

/**
 * Notification callback when executing SQL.
 * @author Nils Kilden-Pedersen
 */
trait ExecutionCallback {
  def beforeExecute(id: Symbol, sql: String)
  def afterExecute(id: Symbol, sql: String, parms: Seq[Any], executionTime: Int)
  def beforeBatchExecute(id: Symbol, sql: String)
  def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int)
  def onWarning(warning: java.sql.SQLWarning, id: Option[Symbol])
}

private[orbroker] object Deaf extends NoOpCallback

/**
 * NoOp callback.
 */
class NoOpCallback extends ExecutionCallback {
  def beforeExecute(id: Symbol, sql: String) {}
  def afterExecute(id: Symbol, sql: String, parms: Seq[Any], executionTime: Int) {}
  def beforeBatchExecute(id: Symbol, sql: String) {}
  def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int) {}
  def onWarning(warning: java.sql.SQLWarning, id: Option[Symbol]) {}
}

/**
 * Ignore certain warnings.
 */
trait IgnoreWarnings extends ExecutionCallback {
  val ignoreSQLState: Set[String]
  abstract override def onWarning(warning: java.sql.SQLWarning, id: Option[Symbol]) {
    if (!ignoreSQLState.contains(warning.getSQLState)) {
      super.onWarning(warning, id)
    }
  }
}

/**
 * Limit callback to slow execution.
 */
trait TimedSQLCallback extends ExecutionCallback {
  
  /**
   * Threshold for callback to propagate.
   */
  val thresholdMillis: Int
  
  abstract override def afterExecute(id: Symbol, sql: String, parms: Seq[Any], executionTime: Int) {
    val millis = executionTime / 1000
    if (millis >= thresholdMillis) {
      super.afterExecute(id, sql, parms, executionTime)
    }
  }

  abstract override def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int) {
    val millis = executionTime / 1000
    if (millis >= thresholdMillis) {
      super.afterBatchExecute(id, sql, parms, executionTime)
    }
  }
  
}
