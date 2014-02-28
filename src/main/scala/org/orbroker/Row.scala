package org.orbroker

/**
 * Row representation. Access SQL data types using either
 * column name or column index (as string). 
 * All values are  wrapped in `Option` to represent nullability.
 * @author Nils Kilden-Pedersen
 */
trait Row extends ReadableMap {
  /**
   * The columns contained in this row. Calling this 
   * method will access result set metadata, which
   * may slow down extraction.
   */
  def columns: Seq[String]

  import java.io.{ Reader, InputStream }

  /**
   * ASCII data stream.
   */
  def asciiStream(name: String): Option[InputStream]
  /**
   * Binary data stream.
   */
  def binaryStream(name: String): Option[InputStream]
  /**
   * Unicode character stream.
   */
  def charStream(name: String): Option[Reader]

}
