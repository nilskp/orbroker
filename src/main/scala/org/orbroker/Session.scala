package org.orbroker

import JdbcCloser._
import org.orbroker.exception._

import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }

/**
 * The session encapsulating the a connection
 * from the data source.
 * @author Nils Kilden-Pedersen
 */
private[orbroker] abstract class Session(
    isolationLevel: Option[Int],
    broker: Broker,
    private val extConn: Option[Connection]) {

  private var conn: Connection = null

  protected def commit() = if (conn != null) conn.commit()
  protected def rollback() = if (conn != null) conn.rollback()

  private[orbroker] def alwaysPrepare = broker.alwaysPrepare

  protected def getStatement(token: Token[_]) = token.getStatement(broker)
  protected def getModStatement(token: Token[_]) = token.getStatement(broker).asInstanceOf[ModifyStatement]
  protected def getCallStatement(token: Token[_]) = token.getStatement(broker).asInstanceOf[CallStatement]

  implicit protected def callback = broker.callback
  protected val readOnly: Boolean
  protected def hasUncommittedChanges: Boolean

  /**
   * Timeout in seconds. Will cause a [[org.orbroker.exception.TimeoutException]]
   * if an execution takes longer than the given time.
   * 0 means no limit.
   * @see java.sql.Statement#setQueryTimeout(int)
   */
  var timeout = broker.timeout

  /**
   * Fetch size in rows.
   * @see java.sql.Statement.setFetchSize(int)
   */
  var fetchSize = broker.fetchSize

  protected[orbroker] final def connection = extConn match {
    case Some(ec) ⇒ ec
    case None ⇒ {
      if (conn == null) {
        conn = broker.newConnection(isolationLevel)
        if (conn.isReadOnly != readOnly) conn.setReadOnly(readOnly)
      }
      conn
    }
  }

  private[orbroker] def discardConnection() {
    if (conn != null) {
      try { conn.close() } catch { case _: Exception ⇒ /* Ignore */ }
      conn = null
    }
  }

  private[orbroker] def close() {
    if (conn != null) {
      try {
        // End potential implicit transaction for drivers that require it before closing
        conn.rollback()
        if (hasUncommittedChanges) {
          throw new RollbackException
        }
      } finally {
        conn.checkAndClose()
      }
    }
  }

  protected final def toMap(args: Iterable[(String, _)]): Map[String, Any] = {
    var map: Map[String, Any] = broker.defaultParms
    args foreach { case arg @ (key, value) ⇒
        if (value.isInstanceOf[Traversable[_]]) {
          val array = value.asInstanceOf[Traversable[Any]].toArray
          map += (key -> array)
        } else {
          map += arg
        }
    }
    map
  }

  protected final def evaluate(id: Symbol, e: SQLException) = {
    if (broker.adapter.isConstraint(e)) {
      new ConstraintException(id, e, broker.adapter.constraintName(e))
    } else if (broker.adapter.isTimeout(e)) {
      new TimeoutException(id, e)
    } else if (broker.adapter.isDeadlock(e)) {
      new DeadlockException(e)
    } else {
      e
    }
  }

}
