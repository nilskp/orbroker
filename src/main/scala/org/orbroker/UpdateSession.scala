package org.orbroker

import java.sql._
import org.orbroker.exception._

private[orbroker] trait UpdateSession extends Executable with QuerySession {
  private def call[T](
    token: Token[T], parms: Seq[(String, Any)],
    keyHandler: Option[(T) => Unit]): Int = {
    val cstm = getCallStatement(token)
    try {
      val (rowsUpdated, _, _) = cstm.call(token, this, toMap(parms), keyHandler, Seq.empty, None)
      uncommittedChanges |= rowsUpdated > 0
      rowsUpdated
    } catch {
      case e: SQLException => throw evaluate(token.id, e)
    }
  }

  def callForUpdate(token: Token[_], parms: (String, Any)*): Int =
    call(token, parms, None)

  def callForKeys[K](token: Token[K], parms: (String, Any)*)(keyHandler: K => Unit): Int = {
    call(token, parms, Some(keyHandler))
  }
}
