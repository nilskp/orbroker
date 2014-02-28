package org.orbroker.config.dynamic

import org.orbroker._
import org.orbroker.adapt._
import org.orbroker.callback._

private[orbroker] trait DynamicSupport {
  def trimSQL: Boolean
  def adapter: BrokerAdapter
  def callback: ExecutionCallback
  protected def dynamicStatement(id: Symbol, sql: String, sqlLines: Seq[String]): Option[SQLStatement]
}