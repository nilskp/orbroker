package org.orbroker.pimp

import org.orbroker._
import java.util.LinkedHashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

/**
 * Collection of pimped methods for queries.
 * @author Nils Kilden-Pedersen
 */
final class PimpedQuery(qry: Queryable) {

  /**
   * Execute unordered query and merge values with identical keys.
   * @param id Query id
   * @param parms Optional parameters
   * @param kf Key function. Get key value from object
   * @param merger Merging function
   * @return Collection of extracted object, in the order first encountered from result set
   */
  def selectUnordered[T](token: Token[T], parms: (String, Any)*)(kf: T ⇒ Any)(merger: (T, T) ⇒ T): Iterable[T] = {
    val map = new LinkedHashMap[Any, T]
    qry.select(token, parms: _*) { rows ⇒
      rows.foreach { t ⇒
        val key = kf(t)
        val ot = map.get(key)
        if (ot != null) { // Replace with merged object
          map.put(key, merger(ot, t))
        } else { // Store new object
          map.put(key, t)
        }
      }
    }
    map.values
  }

  /**
   * Execute query and append result to buffer.
   * @param queryID The SQL statement id
   * @param buffer The buffer to append to
   * @param parms Optional parameters
   * @return Buffer expansion count
   */
  def selectToBuffer[T](token: Token[T], buffer: Buffer[T], parms: (String, Any)*): Int = {
    val preSize = buffer.size
    qry.select(token, parms: _*) { rows ⇒
      rows.foreach { t ⇒
        buffer += t
      }
    }
    buffer.size - preSize
  }

}