package org.orbroker

import org.orbroker.exception.ConfigurationException
import java.sql.CallableStatement
import org.orbroker.exception.NullValueException

final class OutParm private[orbroker] (name: String, cs: CallableStatement, tz: java.util.TimeZone, parmIdxMap: Map[String, Int]) {

  private def getValue[T](extractor: ValueExtractor[T]): T = parmIdxMap.get(name) match {
    case Some(idx) => extractor(idx, cs, tz)
    case None =>
      val msg = s"""Could not find parameter "$name""""
      throw new ConfigurationException(msg)
  }

  @specialized
  def as[T](implicit extractor: ValueExtractor[T]): T = {
    val value = getValue(extractor)
    if (cs.wasNull) {
      throw new NullValueException(name)
    } else {
      value
    }
  }

  def opt[T](implicit extractor: ValueExtractor[T]): Option[T] = {
    val value = getValue(extractor)
    if (cs.wasNull) {
      None
    } else {
      Some(value)
    }
  }

}
