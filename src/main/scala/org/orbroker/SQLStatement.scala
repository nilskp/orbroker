package org.orbroker

import org.orbroker.exception._
import org.orbroker.adapt.BrokerAdapter
import org.orbroker.callback.ExecutionCallback
import java.sql.{ SQLException, ResultSet, Connection, PreparedStatement, CallableStatement }

private[orbroker] abstract class SQLStatement(
    val id: Symbol,
    implicit protected val callback: ExecutionCallback,
    protected val adapter: BrokerAdapter) {

  import SQLStatement._

  protected def statement(parms: Map[String, Any]): ParsedSQL
  protected def diffTimeInMicros(startNanos: Long): Int = ((System.nanoTime - startNanos) / 1000L).asInstanceOf[Int]

  protected final def setParms(token: Token[_], ps: PreparedStatement, parmDefs: IndexedSeq[String], parms: Map[String, _]): Seq[Any] = {
    val values = new ValuesByName(token, parms, mirror)
    ps match {
      case cs: CallableStatement => adapter.setParameters(token.id, cs, parmDefs, values)
      case _ => adapter.setParameters(token.id, ps, parmDefs, values)
    }
  }

}

import org.orbroker.adapt.MetadataAdapter
import java.util.{ Map => JMap, HashMap => JHashMap }

private[orbroker] object SQLStatement {

  private val mirror = new util.Mirror

  val EOL = scala.compat.Platform.EOL

  def parseSQL(id: Symbol, sql: Seq[String], trim: Boolean, adapter: BrokerAdapter): ParsedSQL = {
    val parsed = SQLParser.parse(sql, trim)
    new ParsedSQL(id, parsed.sql.toString, parsed.params, adapter)
  }

  implicit def stringWriterToStringSeq(writer: java.io.StringWriter): Seq[String] = {
    writer.getBuffer.toString.split("[\r\n]").filter(_.trim.length > 0).toSeq
  }

  def toJavaMap(map: Map[String, Any]): JMap[String, Any] = {
    if (map.isEmpty) return java.util.Collections.emptyMap[String, Any]

    val jMap = new JHashMap[String, Any]
    for ((key, value) ← map)
      jMap.put(key, value)
    jMap
  }

  def isProcedureCall(sql: String) = {
    val upper = sql.trim.toUpperCase
    (upper startsWith "CALL") ||
      (upper startsWith "{") ||
      (upper startsWith "EXEC") ||
      ((upper startsWith "BEGIN") && (upper contains "END"))
  }

}