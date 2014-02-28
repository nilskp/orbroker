package org.orbroker.pimp

import org.orbroker._
import org.orbroker.callback._
import org.orbroker.adapt._
import org.orbroker.exception._

import java.sql._

/**
 * Pimped version of [[org.orbroker.OutParms]].
 * @author Nils Kilden-Pedersen
 *
 */
class PimpedOutParms(out: OutParms) {

  /**
   * Extract objects from ResultSet parameter.
   * @param parmName
   * @param extractor
   * @param receiver
   */
  def extract[T, R](parmName: String)(extractor: QueryExtractor[T])(receiver: Iterator[T] ⇒ R): Option[R] = {
    for (rs ← out.any[ResultSet](parmName)) yield extractor match {
      case je: JoinExtractor[_] ⇒ out.mapResult(new SafeJoinExtractor(je), rs, receiver)
      case _ ⇒ out.mapResult(extractor, rs, receiver)
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
    extract(parmName)(extractor) { rows ⇒
      rows.foreach { t ⇒
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
    extract(parmName)(extractor) { rows ⇒
      rows.foldLeft(new scala.collection.mutable.ArrayBuffer[T](64)) {
        case (buffer, t) ⇒
          buffer += t
          buffer
      }
    }.getOrElse(IndexedSeq.empty)
  }

}