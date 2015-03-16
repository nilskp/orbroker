package org.orbroker

private[orbroker] trait StatefulRow extends Row {

  var readable = true
  private var keyValues: Map[String, Any] = null

  def newGroup(newKeyValues: Map[String, Any]) {
    readable = true
    keyValues = newKeyValues
  }

  def matches(compKeyValues: Map[String, Any]) = keyValues.size == compKeyValues.size &&
    keyValues.forall { entry =>
      compKeyValues.getOrElse(entry._1, null) match {
        case ba: Array[Byte] => java.util.Arrays.equals(ba, entry._2.asInstanceOf[Array[Byte]])
        case any => entry._2 == any
      }
    }

  private def exception = throw new IllegalStateException("The first row must be read before mapping rest of group")

  abstract override def apply(column: String)(implicit tz: java.util.TimeZone = null): Column = if (readable) super.apply(column) else exception

}
