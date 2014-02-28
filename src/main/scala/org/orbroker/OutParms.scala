package org.orbroker

import java.sql.{ Blob, Clob, Ref, Date, Time, Timestamp, CallableStatement }
import java.net.URL
import java.io.{ Reader, InputStream }

import org.orbroker.callback._
import org.orbroker.adapt._


/**
 * Representation of output parameters. Names
 * will match whatever the parameter was named
 * in the SQL statement, verbatim.
 * All values are  wrapped in `Option` to represent nullability.
 */
trait OutParms extends ReadableMap with ResultSetProducer

private[orbroker]class OutParmsImpl(
    val id: Symbol,
    parmIdxMap: Map[String, Int], 
    cs: CallableStatement,
    val callback: ExecutionCallback,
    val adapter: BrokerAdapter) extends OutParms {
  override def bit(name: String) = {
    val value = cs.getBoolean(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def tinyInt(name: String) = {
    val value = cs.getByte(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def smallInt(name: String) = {
    val value = cs.getShort(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def integer(name: String) = {
    val value = cs.getInt(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def bigInt(name: String) = {
    val value = cs.getLong(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def real(name: String) = {
    val value = cs.getFloat(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def realDouble(name: String) = {
    val value = cs.getDouble(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def decimal(name: String) = Option(cs.getBigDecimal(parmIdxMap(name)))
  override def string(name: String) = Option(cs.getString(parmIdxMap(name)))
  override def array[T](name: String) = Option(cs.getArray(parmIdxMap(name)).getArray.asInstanceOf[Array[T]])
  override def blob(name: String) = Option(cs.getBlob(parmIdxMap(name)))
  override def clob(name: String) = Option(cs.getClob(parmIdxMap(name)))
  override def binary(name: String) = Option(cs.getBytes(parmIdxMap(name)))
  override def date(name: String) = Option(cs.getDate(parmIdxMap(name)))
  override def date(name: String, tz: java.util.TimeZone) = Option(cs.getDate(parmIdxMap(name), tz))
  override def any[T](name: String) = Option(cs.getObject(parmIdxMap(name)).asInstanceOf[T])
  override def ref(name: String) = Option(cs.getRef(parmIdxMap(name)))
  override def time(name: String) = Option(cs.getTime(parmIdxMap(name)))
  override def time(name: String, tz: java.util.TimeZone) = Option(cs.getTime(parmIdxMap(name), tz))
  override def timestamp(name: String) = Option(cs.getTimestamp(parmIdxMap(name)))
  override def timestamp(name: String, tz: java.util.TimeZone) = Option(cs.getTimestamp(parmIdxMap(name), tz))
  override def dataLink(name: String) = Option(cs.getURL(parmIdxMap(name)))
  
}