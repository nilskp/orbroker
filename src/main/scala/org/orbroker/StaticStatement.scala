package org.orbroker

import org.orbroker.adapt._
import org.orbroker.callback._

private[orbroker]class StaticStatement(
  id: Symbol,
  parsedSQL: ParsedSQL,
  callback: ExecutionCallback,
  adapter: BrokerAdapter)
    extends SQLStatement(id, callback, adapter) {

  def this(id: Symbol, sql: Seq[String], trimSQL: Boolean, 
      callback: ExecutionCallback, adapter: BrokerAdapter) =
        this(id, SQLStatement.parseSQL(id, sql, trimSQL, adapter), callback, adapter)
        
  override protected def statement(parms: Map[String, Any]) = parsedSQL

}
