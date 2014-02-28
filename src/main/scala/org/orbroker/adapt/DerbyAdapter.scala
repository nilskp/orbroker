package org.orbroker.adapt

class DerbyAdapter extends DefaultAdapter with DerbyExceptionAdapter with NullParmAdapter
object DerbyAdapter extends DerbyAdapter

trait DerbyExceptionAdapter extends RegexExceptionAdapter {
  val findName = new scala.util.matching.Regex("""constraint(?: or unique index identified by)? \'(\w+)\'""")
}