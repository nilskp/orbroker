package org.orbroker.config.dynamic

import org.orbroker._
import SQLStatement._
import org.orbroker.adapt.BrokerAdapter
import org.orbroker.callback.ExecutionCallback
import java.io.{ StringReader, StringWriter }
import freemarker.template._

private[orbroker] class FreeMarkerStatement(
  id: Symbol,
  freemarkerSQL: Seq[String],
  trimSQL: Boolean,
  callback: ExecutionCallback,
  adapter: BrokerAdapter,
  config: Configuration)
    extends SQLStatement(id, callback, adapter) {

  import FreeMarkerStatement._

  private val template = {
    val sql = if (usesINPredicate(freemarkerSQL)) {
      val useSquareBrackets = freemarkerSQL.exists { line ⇒
        FindFTLDirective.findFirstMatchIn(line).exists(_.group(1) == "[")
      }
      freemarkerSQL.mkString(EOL) + EOL + (if (useSquareBrackets) SeqExpansionMacroSquare else SeqExpansionMacroAngle)
    } else {
      freemarkerSQL.mkString(EOL)
    }
    new Template(id.name, sql, config)
  }

  override def statement(parms: Map[String, Any]) = {
    val context = toJavaMap(parms)
    val writer = new StringWriter
    template.process(context, writer)
    SQLStatement.parseSQL(id, writer, trimSQL, adapter)
  }
}

private[dynamic] object FreeMarkerStatement {
  private val FindSeqExpansionMacro = """@IN\s+seq=""".r
  def usesINPredicate(sql: Seq[String]) = sql.exists(FindSeqExpansionMacro.pattern.matcher(_).find)
  val SeqExpansionMacroAngle = "<#macro IN seq>IN (<#list .globals[seq] as e><#if (e_index > 0)>,</#if>:${seq}[${e_index}]</#list>)</#macro>"
  val SeqExpansionMacroSquare = "[#macro IN seq]IN ([#list .globals[seq] as e][#if (e_index > 0)],[/#if]:${seq}[${e_index}][/#list])[/#macro]"
  val FindFTLDirective = """([<\[])#ftl""".r
  val isFreeMarkerAvailable = try {
    Class.forName("freemarker.template.Template")
    true
  } catch {
    case _: Throwable ⇒ false
  }
  def hasFreeMarkerConditionals(sq1: String) =
    ((sq1 contains "#if") && (sq1 contains "/#if")) ||
      ((sq1 contains "#list") && (sq1 contains "/#list"))
}