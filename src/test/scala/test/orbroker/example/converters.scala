package test.orbroker.example

import org.orbroker.Row
import org.orbroker.RowExtractor

object IntColumn extends RowExtractor[Int] {
  def extract(row: Row) = row("1").as[Int]
}
