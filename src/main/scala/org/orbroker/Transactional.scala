package org.orbroker

import java.sql.{ Savepoint, Connection }

/**
 * Preferred interface for code taking part in a transaction,
 * without exposing commit or rollback functionality.
 * @author Nils Kilden-Pedersen
 * @see Transactional
 */
class Transaction private[orbroker] (
  isolationLevel: Option[Int],
  broker: Broker,
  extConn: Option[Connection])
    extends Session(isolationLevel, broker, extConn) with UpdateSession {

  protected val readOnly = false

  def makeSavepoint() = connection.setSavepoint
  def rollbackSavepoint(sp: Savepoint) = connection.rollback(sp)
  def releaseSavepoint(sp: Savepoint) = connection.releaseSavepoint(sp)
}

/**
 * Transactional state session. Controls commit and rollback.
 * @author Nils Kilden-Pedersen
 * @see Transaction
 */
final class Transactional private[orbroker] (isolationLevel: Option[Int], broker: Broker)
    extends Transaction(isolationLevel, broker, None) {

  /**
   * Commit current transaction.
   */
  override def commit() {
    uncommittedChanges = false
    super.commit()
  }

  /**
   * Roll back current transaction.
   */
  override def rollback() {
    uncommittedChanges = false
    super.rollback()
  }
}
