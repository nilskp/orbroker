package org.orbroker

import java.sql._
import org.orbroker.exception._

/**
 * The public interface for querying and
 * calling procedures in either
 * read-only mode, or transactional.
 */
trait QuerySession extends Queryable {

  private def call[OP, T, R](
    token: Token[T],
    cstm: CallStatement,
    parms: Seq[(String, Any)],
    receivers: Seq[Iterator[T] ⇒ R],
    outParmHandler: Option[(OutParms) ⇒ OP]): (OP, Seq[R]) = try {
    val (rowsUpdated, outParm, results) =
      cstm.call[OP, T, R](token, this, toMap(parms), None, receivers, outParmHandler)
    if (readOnly && rowsUpdated > 0) {
      throw new StoredProcedureUpdateException(token.id, rowsUpdated)
    }
    outParm -> results
  } catch {
    case e: SQLException ⇒ throw evaluate(token.id, e)
  }

  protected def queryFromCall[T, R](token: Token[T], cs: CallStatement, parms: Seq[(String, Any)], receiver: Iterator[T] ⇒ R): R = {
    token.extractor match {
      case ph: OutParmExtractor[_] ⇒ {
        val pef = (op: OutParms) ⇒ ph.extract(op)
        val (op, _) = call[T, T, Unit](token, cs, parms, Seq.empty, Some(pef))
        receiver(Iterator.single(op))
      }
      case _ ⇒
        val (_, results) = call(token, cs, parms, Seq(receiver), None)
        results.head
    }
  }

  def callForParms[T](token: Token[_], parms: (String, Any)*)(ph: OutParms ⇒ T): T = {
    val cs = getCallStatement(token)
    call[T, Any, Unit](token.asInstanceOf[Token[Any]], cs, parms, Seq.empty, Some(ph))._1
  }
}
