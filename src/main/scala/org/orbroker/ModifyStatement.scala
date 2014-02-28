package org.orbroker

import JdbcCloser._
import org.orbroker.exception._
import org.orbroker.conv._
import org.orbroker.adapt.BrokerAdapter
import org.orbroker.callback.ExecutionCallback
import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement }

private[orbroker] trait ModifyStatement extends SQLStatement with GenKeyProducer {

  def execute[K](token: Token[K], timeout: Int, conn: Connection, parms: Map[String, _], genKeyHandler: Option[K ⇒ Unit]): Int = {
    val parsed = statement(parms)
    val starting = System.nanoTime
    callback.beforeExecute(token.id, parsed.sql)
    val ps = parsed.prepareUpdate(conn, genKeyHandler.isDefined)
    try {
      ps.setQueryTimeout(timeout)
      val values = setParms(token, ps, parsed.parmDefs, parms)
      val rowsUpdated = ps.executeUpdate()
      genKeyHandler.foreach { handleGeneratedKeys(token, _, ps.getGeneratedKeys, rowsUpdated) }
      callback.afterExecute(token.id, parsed.sql, values, diffTimeInMicros(starting))
      rowsUpdated
    } finally {
      ps.checkAndClose(id)
    }
  }

  import scala.collection.mutable.ArrayBuffer

  def executeBatch[G](
    token: Token[G], timeout: Int, conn: Connection, batchParms: (String, Traversable[_]),
    parms: Map[String, _], genKeyHandler: Option[G ⇒ Unit]): Int = {
    val starting = System.nanoTime
    val (batchParmName, batchParmValues) = batchParms
    val parsed = statement(parms)
    callback.beforeBatchExecute(token.id, parsed.sql)
    val ps = parsed.prepareUpdate(conn, genKeyHandler.isDefined)
    val batchValues = new ArrayBuffer[Seq[Any]]
    try {
      ps.setQueryTimeout(timeout)
      batchParmValues foreach { batchParm ⇒
        val allParms = if (batchParmName.length == 0) {
          parms
        } else {
          parms + (batchParmName -> batchParm)
        }
        batchValues += setParms(token, ps, parsed.parmDefs, allParms)
        ps.addBatch()
      }
      if (batchValues.isEmpty) {
        callback.afterBatchExecute(token.id, parsed.sql, batchValues, 0)
        0
      } else {
        val rowsUpdated = ps.executeBatch().view.filter(_ > 0).sum
        genKeyHandler foreach { handleGeneratedKeys(token, _, ps.getGeneratedKeys, rowsUpdated) }
        callback.afterBatchExecute(token.id, parsed.sql, batchValues, diffTimeInMicros(starting))
        rowsUpdated
      }
    } finally {
      ps.checkAndClose(id)
    }
  }

}
