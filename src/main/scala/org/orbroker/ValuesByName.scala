package org.orbroker

private[orbroker] class ValuesByName(token: Token[_], values: Map[String, Any], mirror: util.Mirror)
    extends (String => Any) {

  def apply(name: String): Any = token.convert(mirror.leafValue(values, name))

}
