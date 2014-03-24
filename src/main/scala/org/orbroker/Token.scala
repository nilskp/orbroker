package org.orbroker

import org.orbroker.conv._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.{ Map => ConcurrentMap }

final class Token[T] private (
  private val sql: Option[String],
  val id: Symbol,
  private val ext: QueryExtractor[T],
  private[orbroker] val parmConverters: ConcurrentHashMap[Class[_], Option[ParmConverter]]) {

  private[orbroker] val extractor: QueryExtractor[T] = ext match {
    case je: JoinExtractor[_] ⇒ new SafeJoinExtractor(je)
    case other: QueryExtractor[_] ⇒ other
  }

  private[orbroker] def convert(parm: Any): Any = {
    if (parm == null) {
      null
    } else {
      val refClass = parm.asInstanceOf[AnyRef].getClass
      getConverter(refClass) match {
        case None ⇒ parm
        case Some(pc) ⇒ pc.toJdbcType(parm.asInstanceOf[pc.T])
      }
    }
  }

  private def getConverter(cls: Class[_]): Option[ParmConverter] =
    parmConverters.get(cls) match {
      case null ⇒ fromSuper(cls)
      case o ⇒ o
    }

  private def fromSuper(refClass: Class[_]): Option[ParmConverter] = {
    var cls = refClass.getSuperclass
    // Try all super classes
    while (cls != null)
      parmConverters.get(cls) match {
        case null ⇒ cls = cls.getSuperclass
        case o ⇒ parmConverters.put(refClass, o); return o
      }
    // That didn't work. Let's try the interfaces
    refClass.getInterfaces foreach { iface ⇒
      parmConverters.get(iface) match {
        case pc: Some[_] ⇒
          parmConverters.put(refClass, pc); return pc
        case _ ⇒ // Ignore
      }
    }
    // Nope, nothing can convert. Don't bother doing this again.
    parmConverters.put(refClass, None)
    None
  }

  private[orbroker] def getStatement(broker: Broker) = {
    if (sql.isEmpty)
      broker.getStatement(id)
    else
      broker.makeStatement(id, sql.get)
  }

}

object Token {

  private def toMap(parmConverters: Seq[ParmConverter]) = {
    val map = new ConcurrentHashMap[Class[_], Option[ParmConverter]]
    parmConverters foreach { pc ⇒ map.put(pc.fromType, Some(pc)) }
    map
  }

  def apply[T](sql: String, id: Symbol, extractor: QueryExtractor[T], parmConverters: ParmConverter*) = {
    new Token(Option(sql), id, extractor, toMap(parmConverters))
  }

  def apply[T](id: Symbol, extractor: QueryExtractor[T], parmConverters: ParmConverter*): Token[T] =
    apply(null, id, extractor, parmConverters: _*)

  def apply[T](sql: String, id: Symbol, parmConverters: ParmConverter*) = {
    val extractor = new DefaultExtractor(id).asInstanceOf[RowExtractor[T]]
    new Token(Option(sql), id, extractor, toMap(parmConverters))
  }

  def apply[T](id: Symbol, parmConverters: ParmConverter*): Token[T] =
    apply(null, id, parmConverters: _*)

  def apply[T](sql: String, extractor: QueryExtractor[T], parmConverters: ParmConverter*): Token[T] =
    apply(sql, org.orbroker.NO_ID, extractor, parmConverters: _*)

}