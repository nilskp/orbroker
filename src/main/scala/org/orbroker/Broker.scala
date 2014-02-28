package org.orbroker

import config.BrokerConfig
import org.orbroker.exception.TransientRetryFailedException
import org.orbroker.adapt.BrokerAdapter
import org.orbroker.callback.ExecutionCallback

import java.sql._

object Broker {
  def apply(config: BrokerConfig) = config.build()
}

/**
 * The broker between database and application code.
 * @see [[org.orbroker.config.BrokerConfig]]
 * @author Nils Kilden-Pedersen
 */
final class Broker private[orbroker] (
    private[orbroker] val callback: ExecutionCallback,
    val dataSource: javax.sql.DataSource,
    private[orbroker] val defaultIsolation: Option[Int],
    private[orbroker] val timeout: Int,
    private[orbroker] val fetchSize: Int,
    private[orbroker] val alwaysPrepare: Boolean,
    private[orbroker] val trimSQL: Boolean,
    usernamePassword: Option[(String, String)],
    catalog: Option[String],
    statements: Map[Symbol, SQLStatement],
    private[orbroker] val adapter: BrokerAdapter,
    private[orbroker] val defaultParms: Map[String, Any]) {

  private final val NoneProvided = Int.MinValue + 1

  private[orbroker] def makeStatement(id: Symbol, sql: String) = {
    val sqlLines = sql.split("[\r\n]").filter(_.trim.length > 0).toSeq
    if (SQLStatement.isProcedureCall(sql)) {
      new StaticStatement(id, sqlLines, trimSQL, callback, adapter) with CallStatement
    } else {
      new StaticStatement(id, sqlLines, trimSQL, callback, adapter) with ModifyStatement with QueryStatement
    }
  }

  private[orbroker] def getStatement(id: Symbol) = statements(id)

  private[orbroker] def newConnection(isolationLevel: Option[Int]) = {
    val conn = usernamePassword match {
      case Some((usr, pwd)) ⇒ this.dataSource.getConnection(usr, pwd)
      case None ⇒ this.dataSource.getConnection
    }

    for (cl ← this.catalog if conn.getCatalog != cl) {
      conn.setCatalog(cl)
    }
    for (il ← isolationLevel if conn.getTransactionIsolation != il) {
      conn.setTransactionIsolation(il)
    }
    if (conn.getAutoCommit) {
      conn.setAutoCommit(false)
    }

    conn
  }

  /**
   * Perform read-only behavior, using the given
   * transaction isolation level, or default if left empty.
   * @param isolationLevel The desired transaction isolation level. Leave empty for default.
   * @param f Read-only query block
   */
  def readOnly[T](isolationLevel: Int = NoneProvided)(f: QuerySession ⇒ T): T = {
    readOnly(f, if (isolationLevel == NoneProvided) defaultIsolation else Some(isolationLevel))
  }

  private def readOnly[T](f: QuerySession ⇒ T, isolationLevel: Option[Int]): T = {
    var hasException = false
    val session = new ReadOnly(isolationLevel, this, None)
    try {
      runSession(session, true, f)
    } catch {
      case th ⇒ hasException = true; throw th
    }
    finally {
      try {
        session.close()
      } catch {
        case th if !hasException ⇒ throw th
        case _ ⇒ // Ignore and allow other exception 
      }
    }

  }

  /**
   * Perform read-only behavior, using an externally
   * managed connection. Connection will not be closed.
   * <p>NOTICE: This will not access the data source.
   * @param conn The external connection
   * @param f Read-only query block
   */
  def readOnly[T](conn: Connection)(f: QuerySession ⇒ T): T = {
    val session = new ReadOnly(None, this, Some(conn))
    runSession(session, false, f)
  }

  /**
   * Perform transactional behavior, using the given
   * transaction isolation level, or default if left blank.
   * <p>NOTICE: Remember to either commit or rollback the session.
   * @param isolationLevel The desired transaction isolation level, or empty for default.
   * @param f Transactional execution block
   */
  def transactional[T](isolationLevel: Int = NoneProvided)(f: Transactional ⇒ T): T = {
    transactional(f, if (isolationLevel == NoneProvided) defaultIsolation else Some(isolationLevel))
  }

  private def transactional[T](f: Transactional ⇒ T, isolationLevel: Option[Int]): T = {
    var hasException = false
    val session = new Transactional(isolationLevel, this)
    try {
      runSession(session, true, f)
    } catch {
      case th ⇒ hasException = true; throw th
    }
    finally {
      try {
        session.close()
      } catch {
        case th if !hasException ⇒ throw th
        case _ ⇒ // Ignore and allow other exception 
      }
    }

  }

  /**
   * Perform transactional behavior, using an externally
   * managed connection. The connection will not be closed,
   * nor is `commit` or `rollback` available.
   * <p>NOTICE: This will not access the data source.
   * @param conn The external connection
   * @param f Transaction execution block
   */
  def transactional[T](conn: Connection)(f: Transaction ⇒ T): T = {
    val session = new Transaction(None, this, Some(conn))
    runSession(session, false, f)
  }

  /**
   * Execute and commit a transaction, using the given
   * transaction isolation level, or default if left blank.
   * @param isolationLevel The desired transaction isolation level, or empty for default.
   * @param f Transactional execution block
   */
  def transaction[T](isolationLevel: Int = NoneProvided)(f: Transaction ⇒ T): T = {
    transactional(isolationLevel) { session ⇒
      val t = f(session)
      session.commit()
      t
    }
  }

  private def runSession[T, S <: Session](s: S, retryTransient: Boolean, f: S ⇒ T): T = {
    try {
      f(s)
    } catch {
      case e: SQLException if retryTransient && adapter.isTransient(e) ⇒ {
        s.discardConnection()
        runSession(s, false, f)
      }
      case e: SQLException if !retryTransient && adapter.isTransient(e) ⇒ throw new TransientRetryFailedException(e)
    }
  }

}
