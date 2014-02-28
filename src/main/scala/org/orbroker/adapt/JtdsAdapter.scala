package org.orbroker.adapt

/**
 * Adapter for jTDS driver.
 * @author Nils Kilden-Pedersen
 *
 */
class JtdsAdapter extends DefaultAdapter with JtdsExceptionAdapter
object JtdsAdapter extends JtdsAdapter

trait JtdsExceptionAdapter extends RegexExceptionAdapter {
  val findName = new scala.util.matching.Regex("""(?:(?:CHECK|KEY) constraint|duplicate key.*unique index) ['"](\w+)['"]""")
}