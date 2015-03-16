package org.orbroker

import org.orbroker._
import org.orbroker.callback._
import org.orbroker.adapt._
import org.orbroker.exception._

import java.sql._
import java.util.LinkedHashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

/** Package for pimping O/R Broker. */
package object pimp {
  /**
   * Collection of pimped methods for queries.
   * @author Nils Kilden-Pedersen
   */
  implicit final class PimpedQuery(val qry: Queryable) extends AnyVal {

    /**
     * Execute unordered query and merge values with identical keys.
     * @param id Query id
     * @param parms Optional parameters
     * @param kf Key function. Get key value from object
     * @param merger Merging function
     * @return Collection of extracted object, in the order first encountered from result set
     */
    def selectUnordered[T](token: Token[T], parms: (String, Any)*)(kf: T => Any)(merger: (T, T) => T): Iterable[T] = {
      val map = new LinkedHashMap[Any, T]
      qry.select(token, parms: _*) { rows =>
        rows.foreach { t =>
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
      qry.select(token, parms: _*) { rows =>
        rows.foreach { t =>
          buffer += t
        }
      }
      buffer.size - preSize
    }

  }

  /**
   * Pimped version of [[org.orbroker.OutParms]].
   * @author Nils Kilden-Pedersen
   *
   */
  implicit class PimpedOutParms(val out: OutParms) extends AnyVal {

    /**
     * Extract objects from ResultSet parameter.
     * @param parmName
     * @param extractor
     * @param receiver
     */
    def extract[T, R](parmName: String)(extractor: QueryExtractor[T])(receiver: Iterator[T] => R): Option[R] = {
      for (rs ← out(parmName).opt[ResultSet]) yield extractor match {
        case je: JoinExtractor[_] => out.mapResult(new SafeJoinExtractor(je), rs, receiver)
        case _ => out.mapResult(extractor, rs, receiver)
      }
    }

    /**
     * Extract at most one object from ResultSet parameter.
     * @param parmName
     * @param extractor
     * @return Some object or None if no rows were returned
     */
    @throws(classOf[MoreThanOneException])
    def extractOne[T](parmName: String)(extractor: QueryExtractor[T]): Option[T] = {
      var maybe: Option[T] = None
      extract(parmName)(extractor) { rows =>
        rows.foreach { t =>
          if (maybe.isEmpty) {
            maybe = Some(t)
          } else {
            throw new MoreThanOneException("Statement '%s' with ResultSet parameter \"%s\" returned more than one result".format(out.id.name, parmName))
          }
        }
      }
      maybe
    }

    /**
     * Extract all objects from ResultSet parameter.
     * @param parmName
     * @param extractor
     * @param receiver
     */
    def extractAll[T](parmName: String)(extractor: QueryExtractor[T]): IndexedSeq[T] = {
      extract(parmName)(extractor) { rows =>
        rows.foldLeft(new scala.collection.mutable.ArrayBuffer[T](64)) {
          case (buffer, t) =>
            buffer += t
            buffer
        }
      }.getOrElse(IndexedSeq.empty)
    }

  }
}