package org.orbroker.adapt

/**
 * Adapter for MySQL database.
 */
class MySQLAdapter extends DefaultAdapter with MySQLExceptionAdapter
object MySQLAdapter extends MySQLAdapter

trait MySQLExceptionAdapter extends RegexExceptionAdapter {
  val findName = new scala.util.matching.Regex("""(?:Duplicate entry.*for key (\d+))|(?:foreign key constraint fails.*CONSTRAINT `([^`]+)`)""")
}