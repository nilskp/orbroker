package org.orbroker

/**
 * Row representation. Access SQL data types using either
 * column name or column index (as string).
 * @author Nils Kilden-Pedersen
 */
trait Row {
  /**
   * The columns contained in this row. Calling this
   * method will access result set metadata, which
   * may slow down extraction.
   */
  def columns: Seq[String]

  def apply(column: String)(implicit tz: java.util.TimeZone = null): Column

}
