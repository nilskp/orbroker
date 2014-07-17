package org.orbroker

import exception.ConfigurationException
import adapt.BrokerAdapter
import callback.ExecutionCallback

import java.util.HashMap
import scala.collection.immutable.{ Map ⇒ iMap }
import java.sql.ResultSet

private[orbroker] class JoinGroup private (
    keyColumns: Set[String],
    rs: ResultSet,
    _columnAliases: Map[String, String],
    cache: HashMap[CacheKey, Any],
    adapter: BrokerAdapter) extends Join {

  def this(keyColumns: Set[String], rs: ResultSet, columnAliases: Map[String, String], adapter: BrokerAdapter) =
    this(keyColumns, rs, columnAliases, new HashMap(128), adapter)

  private val columnAliases = toUpper(_columnAliases)

  private def toUpper(map: Map[String, String]): Map[String, String] = {
    map.map(entry ⇒ entry._1.toUpperCase -> entry._2.toUpperCase)
  }

  def newGroup() {
    row.newGroup(getKeyValues)
  }

  val row = new ResultSetRow(rs, adapter, columnAliases) with StatefulRow

  /** Has result set been advanced internally? */
  private var rsAdvanced = false
  def isRsAdvanced = rsAdvanced
  /** Is result set readable at current row? */
  var rsReadable = true

  private def getKeyValues(): iMap[String, Any] = {
    var map: iMap[String, Any] = iMap.empty
    for (column ← keyColumns) {
      val value = rs.getObject(columnAliases.getOrElse(column, column))
      if (value == null) return iMap.empty
      map += (column -> value)
    }
    map
  }

  private def getResultOrNull[T](extractor: JoinExtractor[T], proxy: JoinGroup): T = {
    var result: Any = null
    val key = proxy.getKeyValues()
    if (!key.isEmpty) {
      val cacheKey = new CacheKey(extractor, key)
      result = cache.get(cacheKey)
      if (result == null) {
        result = extractor.extract(proxy.row, proxy)
        cache.put(cacheKey, result)
      }
    }
    result.asInstanceOf[T]
  }

  override def extractOne[T](rawExt: JoinExtractor[T], aliases: Map[String, String]) = {
    val extractor = new SafeJoinExtractor[T](rawExt)
    val proxy = new JoinGroup(extractor.key, rs, aliases, cache, adapter)
    val result = getResultOrNull(extractor, proxy)
    row.readable &= proxy.row.readable
    rsAdvanced = proxy.rsAdvanced
    rsReadable = proxy.rsReadable
    Option(result)
  }

  override def extractGroup[T](ext: QueryExtractor[T], aliases: Map[String, String])(receiver: T ⇒ Unit) {
    this.newGroup()
    ext match {
      case re: RowExtractor[_] ⇒ extractRows(re, aliases, receiver)
      case je: JoinExtractor[_] ⇒ extractJoin(je, aliases, receiver)
      case _ ⇒ throw new ConfigurationException("Cannot extract group using " + ext.getClass.getSimpleName)
    }
  }

  private def extractJoin[T](rawExt: JoinExtractor[T], aliases: Map[String, String], receiver: T ⇒ Unit) {
    val extractor = new SafeJoinExtractor[T](rawExt)
    val proxy = new JoinGroup(extractor.key, rs, aliases ++ columnAliases, cache, adapter)
    do {
      proxy.newGroup()
      val result = getResultOrNull(extractor, proxy)
      if (result != null) {
        receiver(result)
      }
      if (proxy.rsAdvanced) {
        rsReadable = proxy.rsReadable
      } else {
        rsReadable = rs.next
      }
    } while (rsReadable && row.matches(getKeyValues))
    row.readable = false
    rsAdvanced = true
  }

  private def extractRows[T](extractor: RowExtractor[T], aliases: Map[String, String], receiver: T ⇒ Unit) {
    val proxy = new JoinGroup(keyColumns, rs, aliases ++ columnAliases, cache, adapter)
    do {
      proxy.newGroup()
      val result = extractor.extract(proxy.row)
      if (result != null) {
        receiver(result)
      }
      if (proxy.rsAdvanced) {
        rsReadable = proxy.rsReadable
      } else {
        rsReadable = rs.next
      }
    } while (rsReadable && row.matches(getKeyValues))
    row.readable = false
    rsAdvanced = true
  }

}

private final class CacheKey(val extractor: JoinExtractor[_], val keyValues: Map[_, _]) {
  override def hashCode = extractor.hashCode ^ keyValues.hashCode
  override def equals(any: Any) = {
    val that = any.asInstanceOf[CacheKey]
    (this.extractor == that.extractor) && (this.keyValues == that.keyValues)
  }
  override def toString = {
    extractor.getClass.getSimpleName + ":" + keyValues
  }
}
