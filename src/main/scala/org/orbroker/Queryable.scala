package org.orbroker

import java.sql.SQLException
import org.orbroker.exception._
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

private[orbroker] trait Queryable extends Session {

  protected def queryFromSelect[T, R](token: Token[T], qs: QueryStatement, parms: Seq[(String, Any)], receiver: Iterator[T] ⇒ R): R = {
    try {
      qs.query(token, this, toMap(parms), receiver)
    } catch {
      case e: SQLException ⇒ throw evaluate(token.id, e)
    }
  }

  protected def queryFromCall[T, R](token: Token[T], cs: CallStatement, parms: Seq[(String, Any)], receiver: Iterator[T] ⇒ R): R

  /**
   * Execute query without calling back. It is expected that the
   * [[org.orbroker.QueryExtractor]] handles all objects extracted.
   * @param token ID of SQL statement
   * @param parms Parameters for statement, if any
   */
  def selectInto(token: Token[_], parms: (String, Any)*) {
    select(token, parms: _*)((_) ⇒ true)
  }

  /**
   * Execute query and call receiver function per result object.
   * @param token SQL statement token
   * @param parms Parameters for statement, if any
   * @param receiver The result object receiver function, which
   * is expected to return `true` to receive next object, `false` to stop
   */
  def select[T, R](token: Token[T], parms: (String, Any)*)(receiver: Iterator[T] ⇒ R): R = {
    getStatement(token) match {
      case qs: QueryStatement ⇒ queryFromSelect(token, qs, parms, receiver)
      case cs: CallStatement ⇒ queryFromCall(token, cs, parms, receiver)
      case _ ⇒ throw new ConfigurationException("Statement '%s' is not a query".format(token.id.name))
    }
  }

  /**
   * Execute query that will return 0-1 rows
   * (by id, a JOIN of course can return more rows, but only one object).
   * @param id The ID of the statement to execute
   * @param parms Optional SQL parameters
   * @return The selected row, or `None`
   */
  @throws(classOf[MoreThanOneException])
  def selectOne[T](token: Token[T], parms: (String, Any)*): Option[T] = {
    var maybe: Option[T] = None
    select(token, parms: _*) { rows ⇒
      rows.foreach { t ⇒
        if (maybe.isEmpty) {
          maybe = Some(t)
        } else {
          throw new MoreThanOneException(token.id, "returned")
        }
      }
    }
    maybe
  }

  /**
   * Execute query and return sequence of all results.
   * @param token The token of the statement to execute
   * @param parms Optional SQL parameters
   * @return The sequence of results.
   */
  def selectAll[T](token: Token[T], parms: (String, Any)*): IndexedSeq[T] = {
    select(token, parms: _*) { rows ⇒
      rows.foldLeft(new ArrayBuffer[T](64)) {
        case (buffer, t) ⇒
          buffer += t
      }
    }
  }

  /**
   * Execute query and return the top results
   * as defined by the `count` parameter.
   * @param count The top number of results to return
   * @param token The token of the statement to execute
   * @param parms Optional SQL parameters
   * @return The sequence of results.
   */
  def selectTop[T](count: Int, token: Token[T], parms: (String, Any)*): IndexedSeq[T] = {
    if (count <= 0) {
      IndexedSeq.empty
    } else {
      val userFetchSize = fetchSize
      if (userFetchSize == 0 || userFetchSize > count) {
        fetchSize = count
      }
      val buffer = select(token, parms: _*) { rows ⇒
        rows.take(count).foldLeft(new ArrayBuffer[T](count)) {
          case (buffer, t) ⇒ buffer += t
        }
      }
      fetchSize = userFetchSize
      buffer
    }
  }

}
