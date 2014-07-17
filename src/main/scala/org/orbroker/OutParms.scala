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

private[orbroker] class OutParmsImpl(
    val id: Symbol,
    parmIdxMap: Map[String, Int],
    cs: CallableStatement,
    val callback: ExecutionCallback,
    val adapter: BrokerAdapter) extends OutParms {

  private def nullerr(parm: String) = throw new RuntimeException(s"Parameter $parm is NULL")

  override def bit(name: String) = {
    val value = cs.getBoolean(parmIdxMap(name))
    if (cs.wasNull) nullerr(name) else value
  }
  override def tinyInt(name: String) = {
    val value = cs.getByte(parmIdxMap(name))
    if (cs.wasNull) nullerr(name) else value
  }
  override def smallInt(name: String) = {
    val value = cs.getShort(parmIdxMap(name))
    if (cs.wasNull) nullerr(name) else value
  }
  override def integer(name: String) = {
    val value = cs.getInt(parmIdxMap(name))
    if (cs.wasNull) nullerr(name) else value
  }
  override def bigInt(name: String) = {
    val value = cs.getLong(parmIdxMap(name))
    if (cs.wasNull) nullerr(name) else value
  }
  override def real(name: String) = {
    val value = cs.getFloat(parmIdxMap(name))
    if (cs.wasNull) nullerr(name) else value
  }
  override def realDouble(name: String) = {
    val value = cs.getDouble(parmIdxMap(name))
    if (cs.wasNull) nullerr(name) else value
  }
  override def decimal(name: String) =
    cs.getBigDecimal(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value
    }
  override def string(name: String) =
    cs.getString(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value
    }
  override def array[T](name: String) =
    cs.getArray(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value.getArray.asInstanceOf[Array[T]]
    }
  override def blob(name: String) =
    cs.getBlob(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value
    }
  override def clob(name: String) =
    cs.getClob(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value
    }
  override def binary(name: String) =
    cs.getBytes(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value
    }
  override def any[T](name: String) =
    cs.getObject(parmIdxMap(name)).asInstanceOf[T] match {
      case null => nullerr(name)
      case value => value
    }
  override def ref(name: String) =
    cs.getRef(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value
    }
  override def date(name: String, tz: java.util.TimeZone) =
    cs.getDate(parmIdxMap(name), tz) match {
      case null => nullerr(name)
      case value => value
    }
  override def time(name: String, tz: java.util.TimeZone) =
    cs.getTime(parmIdxMap(name), tz) match {
      case null => nullerr(name)
      case value => value
    }
  override def timestamp(name: String, tz: java.util.TimeZone) =
    cs.getTimestamp(parmIdxMap(name), tz) match {
      case null => nullerr(name)
      case value => value
    }
  override def dataLink(name: String) =
    cs.getURL(parmIdxMap(name)) match {
      case null => nullerr(name)
      case value => value
    }

  override def bit_(name: String) = {
    val value = cs.getBoolean(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def tinyInt_(name: String) = {
    val value = cs.getByte(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def smallInt_(name: String) = {
    val value = cs.getShort(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def integer_(name: String) = {
    val value = cs.getInt(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def bigInt_(name: String) = {
    val value = cs.getLong(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def real_(name: String) = {
    val value = cs.getFloat(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def realDouble_(name: String) = {
    val value = cs.getDouble(parmIdxMap(name))
    if (cs.wasNull) None else Some(value)
  }
  override def decimal_(name: String) = Option(cs.getBigDecimal(parmIdxMap(name)))
  override def string_(name: String) = Option(cs.getString(parmIdxMap(name)))
  override def array_[T](name: String) = Option(cs.getArray(parmIdxMap(name)).getArray.asInstanceOf[Array[T]])
  override def blob_(name: String) = Option(cs.getBlob(parmIdxMap(name)))
  override def clob_(name: String) = Option(cs.getClob(parmIdxMap(name)))
  override def binary_(name: String) = Option(cs.getBytes(parmIdxMap(name)))
  override def any_[T](name: String) = Option(cs.getObject(parmIdxMap(name)).asInstanceOf[T])
  override def ref_(name: String) = Option(cs.getRef(parmIdxMap(name)))
  override def date_(name: String, tz: java.util.TimeZone) =
    tz match {
      case null => Option(cs.getDate(parmIdxMap(name)))
      case tz => Option(cs.getDate(parmIdxMap(name), tz))
    }
  override def time_(name: String, tz: java.util.TimeZone) =
    tz match {
      case null => Option(cs.getTime(parmIdxMap(name)))
      case tz => Option(cs.getTime(parmIdxMap(name), tz))
    }
  override def timestamp_(name: String, tz: java.util.TimeZone) =
    tz match {
      case null => Option(cs.getTimestamp(parmIdxMap(name)))
      case tz => Option(cs.getTimestamp(parmIdxMap(name), tz))
    }
  override def dataLink_(name: String) = Option(cs.getURL(parmIdxMap(name)))

}