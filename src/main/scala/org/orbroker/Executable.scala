package org.orbroker

import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }
import org.orbroker.exception._

private[orbroker] trait Executable extends Session {

  protected var uncommittedChanges = false
  override protected def hasUncommittedChanges = uncommittedChanges

  /**
   * Execute a modifying SQL statement, such as INSERT, UPDATE, or DELETE.
   * @param modID The statement id
   * @param parms The parameters
   * @return The number of rows affected
   */
  def execute(token: Token[_], parms: (String, Any)*): Int = execute(token, parms, null)

  def executeForKeys[G](token: Token[G], parms: (String, Any)*)(keyHandler: G => Unit): Int = {
    execute(token, parms, keyHandler)
  }

  private def execute[G](token: Token[G], parms: Seq[(String, Any)], keyHandler: G => Unit): Int = {
    val ms = getModStatement(token)
    val count = try {
      ms.execute(token, timeout, connection, toMap(parms), Option(keyHandler))
    } catch {
      case e: SQLException => throw evaluate(token.id, e)
    }
    uncommittedChanges |= count > 0
    count
  }

  /**
   * Execute a modifying SQL statement, such as INSERT, that results
   * in a generated key row.
   * @param modID The statement id
   * @param parms The parameters
   * @param keyHandler The generated-key callback handler
   * @return The number of rows affected
   */
  def executeForKey[G](token: Token[G], parms: (String, Any)*): Option[G] = {
    var maybe: Option[G] = None
    executeForKeys(token, parms: _*) { key =>
      if (maybe == None) {
        maybe = Some(key)
      } else {
        throw new MoreThanOneException(token.id, "generated")
      }
    }
    maybe
  }

  /**
   * Execute a modifying SQL statement, such as INSERT, UPDATE, DELETE,
   * with a batch of values.
   * @param modID The statement id
   * @param batchValues The batch values
   * @param parms Other, optional, parameters
   * @param keyHandler The generated key callback handler
   * @return The number of rows affected
   */
  def executeBatch(token: Token[_], batchValues: (String, Traversable[_]), parms: (String, Any)*): Int = {
    require(batchValues._1.trim.length > 0, "Batch name cannot be blank")
    executeBatch(token, batchValues, parms, null)
  }
  
  /**
   * Execute a modifying SQL statement, such as INSERT,
   * a specific number of times, as indicated by `batchCount`.
   * @param modID The statement id
   * @param batchValues The batch values
   * @param parms Other, optional, parameters
   * @param keyHandler The generated key callback handler
   * @return The number of rows affected
   */
  def executeBatch(token: Token[_], batchCount: Int, parms: (String, Any)*): Int = {
    val batchValues = new Array[Unit](batchCount).toTraversable
    executeBatch(token, ""->batchValues, parms, null)
  }

  /**
   * Execute a modifying SQL statement, such as INSERT, UPDATE, DELETE,
   * with a batch of values, that results in generated keys.
   * NOTICE: This may not be supported by some JDBC drivers.
   * @param modID The statement id
   * @param batchValues The batch values
   * @param parms Other, optional, parameters
   * @param keyHandler The generated key callback handler
   * @return The number of rows affected
   */
  def executeBatchForKeys[G](token: Token[G], batchValues: (String, Traversable[_]), parms: (String, Any)*)(keyHandler: G => Unit): Int = {
    require(batchValues._1.trim.length > 0, "Batch name cannot be blank")
    executeBatch(token, batchValues, parms, keyHandler)
  }
  
  /**
   * Execute a modifying SQL statement, such as INSERT,
   * a specific number of times, as indicated by `batchCount`.
   * NOTICE: This may not be supported by some JDBC drivers.
   * @param modID The statement id
   * @param batchCount The batch count
   * @param parms Other, optional, parameters
   * @param keyHandler The generated key callback handler
   * @return The number of rows affected
   */
  def executeBatchForKeys[G](token: Token[G], batchCount: Int, parms: (String, Any)*)(keyHandler: G => Unit): Int = {
    val batchValues = new Array[Unit](batchCount).toTraversable
    executeBatch(token, ""->batchValues, parms, keyHandler)
  }

  private def executeBatch[G](token: Token[G], batchValues: (String, Traversable[_]), parms: Seq[(String, Any)], keyHandler: G => Unit): Int = {
    val ms = getModStatement(token)
    val count = try {
      ms.executeBatch(token, timeout, connection, batchValues, toMap(parms), Option(keyHandler))
    } catch {
      case e: SQLException => throw evaluate(token.id, e)
    }
    uncommittedChanges |= count > 0
    count
  }

}
