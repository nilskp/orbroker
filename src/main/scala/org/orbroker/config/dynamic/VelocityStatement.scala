package org.orbroker.config.dynamic

import org.orbroker.SQLStatement
import SQLStatement._
import org.orbroker.exception._
import org.orbroker.conv._
import org.orbroker.adapt.BrokerAdapter
import org.orbroker.callback.ExecutionCallback

import org.apache.velocity._
import org.apache.velocity.app.VelocityEngine
import org.apache.velocity.context._
import org.apache.velocity.runtime.RuntimeInstance
import org.apache.velocity.runtime.parser.node.SimpleNode
import java.io.StringWriter

private[orbroker] class VelocityStatement(
    id: Symbol,
    velocitySQL: Seq[String],
    trimSQL: Boolean,
    callback: ExecutionCallback,
    adapter: BrokerAdapter,
    runtime: RuntimeInstance) extends SQLStatement(id, callback, adapter) {

  private val usesINPredicate = VelocityStatement.usesINPredicate(velocitySQL)

  private val sqlString = if (usesINPredicate) {
    velocitySQL.mkString(EOL) + EOL + VelocityStatement.SeqExpansionMacro
  } else {
    velocitySQL.mkString(EOL)
  }

  private val template = new ThreadLocal[SimpleNode] {
    override def initialValue = {
      val temp = runtime.parse(sqlString, id.name)
      temp.init(null, runtime)
      temp
    }
  }

  override def statement(parms: Map[String, Any]) = {
    val context = new VelocityContext(toJavaMap(parms))
    if (usesINPredicate) {
      context.put("_ctx_", context)
    }
    val ctxAdapter = new InternalContextAdapterImpl(context)
    val writer = new StringWriter
    template.get.render(ctxAdapter, writer)
    SQLStatement.parseSQL(id, writer, trimSQL, adapter)
  }
}

private[dynamic] object VelocityStatement {
  private val FindSeqExpansionMacro = """#IN\s*\(""".r
  def usesINPredicate(sql: Seq[String]) = sql.exists(FindSeqExpansionMacro.pattern.matcher(_).find)
  val SeqExpansionMacro = "#macro(IN $seqName)IN (#foreach($e in $_ctx_[$seqName]):${seqName}[$foreach.index]#if($foreach.hasNext),#end#end)#end"
  val isVelocityAvailable = try {
    Class.forName("org.apache.velocity.app.VelocityEngine")
    true
  } catch {
    case _ ⇒ false
  }
  def hasVelocityConditionals(sql: String) =
    ((sql contains "#if") || (sql contains "#foreach")) &&
      (sql contains "#end")
}

