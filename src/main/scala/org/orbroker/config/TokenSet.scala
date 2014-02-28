package org.orbroker.config

import org.orbroker.Token 
import org.orbroker.exception._

/**
 * Utility class for token id verification.
 * @see BrokerConfig#verify(Set[Symbol])
 * @author Nils Kilden-Pedersen
 *
 */
abstract class TokenSet(nameMustMatchID: Boolean) {
  /**
   * Set of ids defined in this class
   */
  lazy val idSet: Set[Symbol] = {
    var set: Set[Symbol] = Set.empty
    for (method ← getClass.getMethods if method.getParameterTypes.length == 0) {
      if (method.getReturnType == classOf[Token[_]]) {
        val token = method.invoke(TokenSet.this).asInstanceOf[Token[_]]
        if (nameMustMatchID && method.getName != token.id.name) {
          throw nameMismatchException(token.id, method)
        }
        set += token.id
      } else if (method.getReturnType == classOf[Symbol]) {
        val id = method.invoke(TokenSet.this).asInstanceOf[Symbol]
        if (nameMustMatchID && method.getName != id.name) {
          throw nameMismatchException(id, method)
        }
        set += id
      }
    }
    set
  }

  private def nameMismatchException(id: Symbol, method: java.lang.reflect.Method) = {
    val msg = method.getDeclaringClass.getName + "." + method.getName + " does not match id: " + id.name
    throw new ConfigurationException(msg)
  }

}
