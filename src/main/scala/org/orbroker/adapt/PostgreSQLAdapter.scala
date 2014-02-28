/*
 * Created by IntelliJ IDEA.
 * User: Nikolay.Ustinov
 * Date: 10.02.11
 * Time: 22:19
 */
package org.orbroker.adapt

/**
 * Adapter for PostgreSQL.
 */
class PostgreSQLAdapter extends DefaultAdapter with PostgreSQLExceptionAdapter with NullParmAdapter
object PostgreSQLAdapter extends PostgreSQLAdapter

trait PostgreSQLExceptionAdapter extends RegexExceptionAdapter {
  // Matches UK violating, CHECK violating, FK violating (parent key not found)
  val findName = new scala.util.matching.Regex("""violates (?:check|unique|foreign key) constraint ["'](\w+)["']""")
}
