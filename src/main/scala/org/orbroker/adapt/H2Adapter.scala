package org.orbroker.adapt

/**
 * Adapter for H2 database.
 */
class H2Adapter extends DefaultAdapter with H2ExceptionAdapter
object H2Adapter extends H2Adapter

trait H2ExceptionAdapter extends RegexExceptionAdapter {
  val findName = new scala.util.matching.Regex("""violation: "(\w+) ON""")
}