package org.orbroker.util

import org.orbroker.{ Row, RowExtractor }

/**
 * A no-op extractor that allows on-the-fly
 * inline column extraction.
 */
object InlineExtractor extends RowExtractor[Row] {
  def extract(row: Row) = row
}
