package org.orbroker.config.dynamic

import org.orbroker._
import org.apache.velocity.runtime.RuntimeInstance

/**
 * Velocity support for [[org.orbroker.config.BrokerConfig]].
 * @author Nils Kilden-Pedersen
 */
trait VelocitySupport extends DynamicSupport {

  /** Optional template engine properties. */
  var velocityProps: Map[String, String] = Map()

  private lazy val velocityRuntime: RuntimeInstance = {
    val props = new java.util.Properties
    for ((key, value) ← velocityProps)
      props.setProperty(key, value)
    val runtime = new RuntimeInstance
    runtime.init(props)
    runtime
  }

  protected abstract override def dynamicStatement(id: Symbol, sql: String, sqlLines: Seq[String]): Option[SQLStatement] = {
    val superDynamic = super.dynamicStatement(id, sql, sqlLines)
    if (superDynamic == None && isVelocity(sql, sqlLines)) {
      Some(new VelocityStatement(id, sqlLines, trimSQL, callback, adapter, velocityRuntime) with ModifyStatement with QueryStatement)
    } else {
      superDynamic
    }
  }

  import VelocityStatement._
  private def isVelocity(sql: String, sqlLines: Seq[String]) = isVelocityAvailable && (hasVelocityConditionals(sql) || sql.contains("$") || usesINPredicate(sqlLines))

}