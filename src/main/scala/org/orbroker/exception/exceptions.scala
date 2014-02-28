package org.orbroker.exception

import java.sql.SQLException
import scala.collection.Set

/**
 * Top level exception for O/R Broker.
 */
abstract class BrokerException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

/**
 * More than one row was returned or affected by the executed statement.
 * A query meant to return just one row, returned more than one row.
 */
class MoreThanOneException(message: String) extends BrokerException(message, null) {
  def this(id: Symbol, pastTense: String) = this("Statement '%s' %s more than one row".format(id.name, pastTense))
}

/**
 * Exception thrown when a constraint was violated.
 * @param name Constraint name, if available
 */
class ConstraintException(id: Symbol, cause: SQLException, val name: Option[String] = None) extends BrokerException("Constraint violation when executing '%s'".format(id.name), cause)

/**
 * A transaction was not committed, hence it was rolled back.
 */
class RollbackException extends BrokerException("Uncommitted transaction was rolled back", null)

/**
 * A [[java.sql.SQLException]] was identified as transient by the [[org.orbroker.adapt.ExceptionAdapter]],
 * and the action was retried with a new connection, but failed again.
 */
class TransientRetryFailedException(cause: SQLException) extends BrokerException("Failed retrying supposedly transient problem", cause)

/**
 * A deadlock was detected.
 */
class DeadlockException(cause: SQLException) extends BrokerException(cause.getMessage, cause)

/**
 * An operation timed out before completing.
 */
class TimeoutException(id: Symbol, cause: SQLException) extends BrokerException("Statement '%s' timed out".format(id.name), cause)

/**
 * Some operation was attempted on the JDBC API, but is unsupported by
 * the JDBC driver implementation.
 */
class UnsupportedJDBCOperationException(msg: String) extends BrokerException(msg, null)

/**
 * A stored procedure, which performed updating, was called in a read-only session.
 */
class StoredProcedureUpdateException(id: Symbol, rowCount: Int) extends BrokerException("Stored procedure '%s' updated %d rows, but was executed in read-only mode".format(id.name, rowCount), null)

/**
 * Some error was detected in the configuration.
 */
class ConfigurationException(msg: String, cause: Throwable) extends BrokerException(msg, cause) {
  def this(msg: String) = this(msg, null)
}

/**
 * Expected statements were missing from builder.
 */
class MissingStatementException(val missing: Set[Symbol]) extends ConfigurationException("Expected statements missing: %s".format(missing.mkString(", ")))
