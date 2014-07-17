package org.orbroker.util

import org.orbroker._

/**
 * Extract each row into a Map.
 */
object MapExtractor extends RowExtractor[Map[String, Any]] {
  def extract(row: Row) = {
    var map = new scala.collection.immutable.HashMap[String, Any]
    row.columns foreach { name ⇒
      row.any_(name) foreach { value: Any ⇒
        map += name -> value
      }
    }
    map
  }
}