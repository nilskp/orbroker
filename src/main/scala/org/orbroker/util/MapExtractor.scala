package org.orbroker.util

import org.orbroker._

/**
 * Extract each row into a Map.
 */
object MapExtractor extends RowExtractor[Map[String, Any]] {
  def extract(row: Row) = {
    row.columns.foldLeft(new scala.collection.immutable.HashMap[String, Any]) {
      case (map, name) => row(name).opt[Any] match {
        case Some(value) => map.updated(name, value)
        case _ => map
      }
    }
  }
}