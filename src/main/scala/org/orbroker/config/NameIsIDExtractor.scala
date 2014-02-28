package org.orbroker.config

private[config] object NameIsIDExtractor extends Function1[String, Symbol] {
  def apply(name: String): Symbol = {
    val lastDot = name.lastIndexOf('.')
    if (lastDot < 1) 
      Symbol(name)
    else 
      Symbol(name.substring(0, lastDot))
  }
}

private[config] object IDIsNameExtractor extends Function1[Symbol, String] {
  def apply(id: Symbol): String = id.name.concat(".sql")
}