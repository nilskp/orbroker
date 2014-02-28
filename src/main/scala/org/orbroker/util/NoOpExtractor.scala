package org.orbroker.util

import org.orbroker.{ Row, RowExtractor }

/**
 * A no-op extractor that allows on-the-fly
 * row extraction.
 */
object NoOpExtractor extends RowExtractor[Row] {
  def extract(row: Row) = row
}