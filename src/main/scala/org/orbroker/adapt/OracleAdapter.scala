/*
 * Created by IntelliJ IDEA.
 * User: Nikolay.Ustinov
 * Date: 10.02.11
 * Time: 21:48
 */
package org.orbroker.adapt

class OracleAdapter extends DefaultAdapter with OracleExceptionAdapter
object OracleAdapter extends OracleAdapter 

trait OracleExceptionAdapter extends RegexExceptionAdapter {
  // Matches UK violating, CHECK violating, FK violating (parent key not found)
  // Constraint name is returned with username (USERNAME.CONSTRAINT_NAME)
  val findName = new scala.util.matching.Regex("""ORA-(?:00001|02290|02291)\:.*constraint \((.+)\) violated""")
}