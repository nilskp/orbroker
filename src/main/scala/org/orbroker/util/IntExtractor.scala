package org.orbroker.util

import org.orbroker._

/**
 * Extract single Int column.
 */
object IntExtractor extends RowExtractor[Int] {
  def extract(row: Row) = row("1").as[Int]
}