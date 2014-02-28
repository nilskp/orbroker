package org.orbroker

import java.sql.Connection

/**
 * Read-only session. Allows queries and stored procedure calls
 * that return result sets. A stored procedure that performs updates
 * is not allowed through this interface.
 */
private[orbroker] final class ReadOnly(
  isolationLevel: Option[Int],
  broker: Broker,
  extConn: Option[Connection])
    extends Session(isolationLevel, broker, extConn) with QuerySession {

  override protected val readOnly = true

  override protected def hasUncommittedChanges = false

}
