package org.orbroker.adapt

import java.sql.SQLException

/**
 * Adapter for [[java.sql.SQLException]]. Used to determine
 * which exceptions to rethrow as more specific exceptions,
 * which can be useful to handle.
 */
trait ExceptionAdapter {

  /**
   * Return SQLState.
   * @param e
   * @return a SQLState code of length 5 or <code>None</code> if no 5 char state was found.
   */
  protected final def state(e: SQLException): Option[String] = {
    var state = e.getSQLState
    if (state == null) {
      None
    } else {
      state = state.trim.toUpperCase
      if (state.length != 5) None else Some(state)
    }
  }

  /**
   * @param source
   * @return 2 char SQLState class or <code>None</code> if such does not exist.
   */
  protected final def stateClass(e: SQLException): Option[String] = {
    this.state(e) match {
      case None ⇒ None
      case Some(state) ⇒ Some(state.substring(0, 2))
    }
  }

  def isConstraint(e: SQLException): Boolean
  def isTransient(e: SQLException): Boolean
  def isDeadlock(e: SQLException): Boolean
  def isTimeout(e: SQLException): Boolean

  def constraintName(e: SQLException): Option[String]
}

/**
 * Default exception adapter.
 */
trait DefaultExceptionAdapter extends ExceptionAdapter {

  private val ConstraintCodes = Seq("23")
  private val DeadlockCodes = Seq("40001", "57033")
  private val TransientCodes = Seq("08")
  private val TimeoutCodes = Seq("57005", "HYT00", "HYT01", "S1T00")

  private def matchState(options: Seq[String], source: SQLException): Boolean = {
    state(source) match {
      case Some(state) ⇒ for (option ← options) if (state startsWith option) return true
      case None ⇒ return false
    }
    false
  }

  def isConstraint(e: SQLException) = matchState(ConstraintCodes, e)
  def isTransient(e: SQLException) = matchState(TransientCodes, e)
  def isDeadlock(e: SQLException) = matchState(DeadlockCodes, e)
  def isTimeout(e: SQLException) = matchState(TimeoutCodes, e)

  def constraintName(e: SQLException): Option[String] = None

}