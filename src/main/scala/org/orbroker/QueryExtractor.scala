package org.orbroker

import adapt._
import exception._

import java.sql.ResultSet

/**
 * Query extractor. This is a unifying type for
 * [[org.orbroker.RowExtractor]] and [[org.orbroker.JoinExtractor]]. This
 * should not be implemented directly.
 * @author Nils Kilden-Pedersen
 */
sealed trait QueryExtractor[T] {
  private[orbroker] def mapResultSet[R](rs: ResultSet, receiver: Iterator[T] => R, adapter: BrokerAdapter): R
}

/**
 * Interface for extracting user defined object
 * from a single row.
 * <p>Implement this row extractor if a query is a simple
 * non-JOIN query and the type extracted will not be
 * extracted as part of a JOIN from another query.
 * @see JoinExtractor
 * @author Nils Kilden-Pedersen
 */
trait RowExtractor[T] extends QueryExtractor[T] {

  def extract(row: Row): T

  private[orbroker] override def mapResultSet[R](rs: ResultSet, receiver: Iterator[T] => R, adapter: BrokerAdapter): R = {
    receiver(new RowIterator(rs, adapter, extract))
  }
}

trait OutParmExtractor[T] extends QueryExtractor[T] {
  def extract(out: OutParms): T

  private[orbroker] override final def mapResultSet[R](rs: ResultSet, receiver: Iterator[T] => R, adapter: BrokerAdapter): R =
    throw new IllegalArgumentException("Cannot extract ResultSet using " + getClass)
}

/**
 * Interface for extracting user defined object
 * from a group of rows.
 * Implement this join extractor if a query is a
 * JOIN query <em>or</em> if this type needs to
 * be extracted from another JOIN query.
 * <p>NOTICE: Extraction should be done in
 * the following sequence:
 * <ol>
 * <li>[[org.orbroker.Row]]</li>
 * <li>[[org.orbroker.Join.extractOne]]</li>
 * <li>[[org.orbroker.Join.extractGroup]] (or [[org.orbroker.Join.extractSeq]])</li>
 * </ol>
 * @see RowExtractor
 * @author Nils Kilden-Pedersen
 */
trait JoinExtractor[T] extends QueryExtractor[T] {
  /**
   * The set of columns that uniquely distinguishes
   * this object in a result set, typically the columns
   * that compose the primary key. The query should be
   * ordered by those columns.
   */
  def key: Set[String]
  def extract(row: Row, join: Join): T

  private[orbroker] override final def mapResultSet[R](rs: ResultSet, receiver: Iterator[T] => R, adapter: BrokerAdapter): R = {
    receiver(new JoinIterable(key, rs, adapter, extract).iterator)
  }

}

private[orbroker] final class DefaultExtractor(id: Symbol) extends RowExtractor[Any] {
  def extract(row: Row): Any = try {
    row.columns.size match {
      case 1 => row("1").opt[Any].orNull
      case 2 => (row("1").opt[Any].orNull, row("2").opt[Any].orNull)
      case 3 => (row("1").opt[Any].orNull, row("2").opt[Any].orNull, row("3").opt[Any].orNull)
      case 4 => (row("1").opt[Any].orNull, row("2").opt[Any].orNull, row("3").opt[Any].orNull, row("4").opt[Any].orNull)
      case 5 => (row("1").opt[Any].orNull, row("2").opt[Any].orNull, row("3").opt[Any].orNull, row("4").opt[Any].orNull, row("5").opt[Any].orNull)
      case x => throw new ConfigurationException(s"$x columns available for '$id', and no RowExtractor registered")
    }
  } catch {
    case e: NoSuchElementException => throw new ConfigurationException(s"Statement '$id' contains NULL values. Must register a RowExtractor")
  }
}

private[orbroker] final class SafeJoinExtractor[T](val delegate: JoinExtractor[T]) extends JoinExtractor[T] {
  require(!delegate.key.isEmpty, "No columns defined for key")
  val key = delegate.key.map(_.toUpperCase)
  def extract(row: Row, join: Join): T = delegate.extract(row, join)
  override def equals(any: Any) = this.delegate eq any.asInstanceOf[SafeJoinExtractor[T]].delegate
  override def hashCode = this.delegate.hashCode
}
