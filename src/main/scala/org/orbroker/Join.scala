package org.orbroker

import scala.collection.immutable.{ Map => iMap }

/**
 * The JOIN part of a query. Use this to extract
 * other objects, one-to-one or one-to-many, from
 * the result, by reusing other extractors.
 * @author Nils Kilden-Pedersen
 */
trait Join {
  private[orbroker] val row: Row

  /**
   * Leverage another extractor to extract secondary result object,
   * typically from a 1-to-1 JOIN, providing column aliasing 
   * from column name collisions, if needed.
   * @param extractor The query extractor
   * @param aliasMap Map of column name to column rename
   * @return result object
   */
  def extractOne[T](extractor: JoinExtractor[T], aliasMap: Map[String, String] = iMap.empty): Option[T]

  /**
   * Extract the rest of the rows remaining in this group of rows, 
   * using another extractor to build a collection of 
   * secondary result objects, typically from a 1-to-many JOIN, 
   * providing column aliasing from column name collisions, if needed.
   * @param extractor The query extractor
   * @param columnAlias Map of column name to column rename
   * @param receiver The receiving function per result object
   */
  def extractGroup[T](extractor: QueryExtractor[T], columnAlias: Map[String, String] = iMap.empty)(receiver: T => Unit)

  /**
   * Extract the rest of the rows remaining in this group of rows, 
   * using another extractor to build a [[scala.collection.IndexedSeq]] of 
   * secondary result objects, typically from a 1-to-many JOIN, 
   * providing column aliasing from column name collisions, if needed.
   * @param extractor The query extractor
   * @param columnAlias Map of column name to column rename
   * @return The sequence of objects
   */
  def extractSeq[T](extractor: QueryExtractor[T], columnAlias: Map[String, String] = iMap.empty): IndexedSeq[T] = {
    val buffer = new scala.collection.mutable.ArrayBuffer[T](64)
    extractGroup(extractor, columnAlias) { t: T => buffer += t }
    buffer
  }
}
