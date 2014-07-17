package org.orbroker

private[orbroker] trait StatefulRow extends Row {

  var readable = true
  private var keyValues: Map[String, Any] = null

  def newGroup(newKeyValues: Map[String, Any]) {
    readable = true
    keyValues = newKeyValues
  }

  def matches(compKeyValues: Map[String, Any]) = keyValues.size == compKeyValues.size &&
    keyValues.forall { entry ⇒
      compKeyValues.getOrElse(entry._1, null) match {
        case ba: Array[Byte] ⇒ java.util.Arrays.equals(ba, entry._2.asInstanceOf[Array[Byte]])
        case any ⇒ entry._2 == any
      }
    }

  private def exception = throw new IllegalStateException("The first row must be read before mapping rest of group")

  abstract override def bit(column: String) = if (readable) super.bit(column) else exception
  abstract override def tinyInt(column: String) = if (readable) super.tinyInt(column) else exception
  abstract override def smallInt(column: String) = if (readable) super.smallInt(column) else exception
  abstract override def integer(column: String) = if (readable) super.integer(column) else exception
  abstract override def bigInt(column: String) = if (readable) super.bigInt(column) else exception
  abstract override def real(column: String) = if (readable) super.real(column) else exception
  abstract override def realDouble(column: String) = if (readable) super.realDouble(column) else exception
  abstract override def decimal(column: String) = if (readable) super.decimal(column) else exception
  abstract override def string(column: String) = if (readable) super.string(column) else exception
  abstract override def array[T](column: String) = if (readable) super.array(column) else exception
  abstract override def asciiStream(column: String) = if (readable) super.asciiStream(column) else exception
  abstract override def binaryStream(column: String) = if (readable) super.binaryStream(column) else exception
  abstract override def blob(column: String) = if (readable) super.blob(column) else exception
  abstract override def clob(column: String) = if (readable) super.clob(column) else exception
  abstract override def charStream(column: String) = if (readable) super.charStream(column) else exception
  abstract override def binary(column: String) = if (readable) super.binary(column) else exception
  abstract override def any[T](column: String) = if (readable) super.any(column) else exception
  abstract override def ref(column: String) = if (readable) super.ref(column) else exception
  abstract override def dataLink(column: String) = if (readable) super.dataLink(column) else exception
  abstract override def date(column: String, tz: java.util.TimeZone) = if (readable) super.date(column, tz) else exception
  abstract override def time(column: String, tz: java.util.TimeZone) = if (readable) super.time(column, tz) else exception
  abstract override def timestamp(column: String, tz: java.util.TimeZone) = if (readable) super.timestamp(column, tz) else exception

  abstract override def bit_(column: String) = if (readable) super.bit_(column) else exception
  abstract override def tinyInt_(column: String) = if (readable) super.tinyInt_(column) else exception
  abstract override def smallInt_(column: String) = if (readable) super.smallInt_(column) else exception
  abstract override def integer_(column: String) = if (readable) super.integer_(column) else exception
  abstract override def bigInt_(column: String) = if (readable) super.bigInt_(column) else exception
  abstract override def real_(column: String) = if (readable) super.real_(column) else exception
  abstract override def realDouble_(column: String) = if (readable) super.realDouble_(column) else exception
  abstract override def decimal_(column: String) = if (readable) super.decimal_(column) else exception
  abstract override def string_(column: String) = if (readable) super.string_(column) else exception
  abstract override def array_[T](column: String) = if (readable) super.array_(column) else exception
  abstract override def asciiStream_(column: String) = if (readable) super.asciiStream_(column) else exception
  abstract override def binaryStream_(column: String) = if (readable) super.binaryStream_(column) else exception
  abstract override def blob_(column: String) = if (readable) super.blob_(column) else exception
  abstract override def clob_(column: String) = if (readable) super.clob_(column) else exception
  abstract override def charStream_(column: String) = if (readable) super.charStream_(column) else exception
  abstract override def binary_(column: String) = if (readable) super.binary_(column) else exception
  abstract override def any_[T](column: String) = if (readable) super.any_(column) else exception
  abstract override def ref_(column: String) = if (readable) super.ref_(column) else exception
  abstract override def dataLink_(column: String) = if (readable) super.dataLink_(column) else exception
  abstract override def date_(column: String, tz: java.util.TimeZone) = if (readable) super.date_(column, tz) else exception
  abstract override def time_(column: String, tz: java.util.TimeZone) = if (readable) super.time_(column, tz) else exception
  abstract override def timestamp_(column: String, tz: java.util.TimeZone) = if (readable) super.timestamp_(column, tz) else exception

}
