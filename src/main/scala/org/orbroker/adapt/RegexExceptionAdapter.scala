package org.orbroker.adapt

/**
 * @author Nils Kilden-Pedersen
 */
trait RegexExceptionAdapter extends DefaultExceptionAdapter {

  val findName: scala.util.matching.Regex

  override final def constraintName(e: java.sql.SQLException): Option[String] = {
    findName.findFirstMatchIn(e.getMessage).flatMap(m => Option(m.group(1)))
  }
}