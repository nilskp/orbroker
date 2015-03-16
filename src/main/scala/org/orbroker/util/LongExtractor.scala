package org.orbroker.util

import org.orbroker._

/**
 * Extract single Int column.
 */
object LongExtractor extends RowExtractor[Long] {
  def extract(row: Row) = row("1").as[Long]
}