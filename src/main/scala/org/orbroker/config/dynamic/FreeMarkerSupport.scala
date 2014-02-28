package org.orbroker.config.dynamic

import org.orbroker._
import freemarker.template.Configuration

/**
 * FreeMarker support for [[org.orbroker.config.BrokerConfig]].
 * @author Nils Kilden-Pedersen
 */
trait FreeMarkerSupport extends DynamicSupport {

  /** Optional template engine properties. */
  var freemarkerProps: Map[String, String] = Map("number_format" -> "0")

  private lazy val freeMarkerConfig: Configuration = {
    val config = new Configuration
    for ((key, value) ← freemarkerProps)
      config.setSetting(key, value)
    config
  }

  protected abstract override def dynamicStatement(id: Symbol, sql: String, sqlLines: Seq[String]): Option[SQLStatement] = {
    val superDynamic = super.dynamicStatement(id, sql, sqlLines)
    if (superDynamic == None && isFreeMarker(sql, sqlLines)) {
      Some(new FreeMarkerStatement(id, sqlLines, trimSQL, callback, adapter, freeMarkerConfig) with ModifyStatement with QueryStatement)
    } else {
      superDynamic
    }
  }

  import FreeMarkerStatement._
  
  private def isFreeMarker(sql: String, sqlLines: Seq[String]) = isFreeMarkerAvailable && (hasFreeMarkerConditionals(sql) || sql.contains("${") || usesINPredicate(sqlLines))

}