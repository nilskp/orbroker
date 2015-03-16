package org.orbroker

import org.orbroker.exception._
import org.orbroker.adapt._
import scala.collection.immutable.IndexedSeq

import java.sql.{ Connection, SQLException, PreparedStatement, CallableStatement }
import java.sql.ResultSet._

private[orbroker] final class ParsedSQL(
    val id: Symbol,
    val sql: String,
    val parmDefs: IndexedSeq[String],
    adapter: BrokerAdapter) {

  def createStatement(conn: Connection) = try {
    require(parmDefs.isEmpty, "Must prepare statement when parameters exist")
    conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)
  } catch {
    case e: SQLException => throw preparationError(e)
  }

  def prepareQuery(conn: Connection) = try {
    conn.prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)
  } catch {
    case e: SQLException => throw preparationError(e)
  }

  def prepareUpdate(conn: Connection, genKeys: Boolean) = try {
    adapter.prepareUpdate(id, conn, sql, genKeys)
  } catch {
    case e: SQLException => throw preparationError(e)
  }

  lazy val parmIdxMap: Map[String, Int] = {
    var map: Map[String, Int] = Map.empty
    var i = 0
    for (parm ← parmDefs) {
      i += 1
      map += (parm -> i)
    }
    map
  }

  def prepareCall(conn: Connection, hasResultSet: Boolean) = try {
    val cs = if (hasResultSet) {
      conn.prepareCall(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)
    } else {
      conn.prepareCall(sql)
    }
    cs
  } catch {
    case e: SQLException => throw preparationError(e)
  }

  private def preparationError(e: SQLException) = {
    new ConfigurationException("Failed to prepare statement:%n%s".format(sql), e)
  }

  override def toString = sql

}
