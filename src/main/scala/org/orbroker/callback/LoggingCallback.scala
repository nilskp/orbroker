package org.orbroker.callback

import org.orbroker.NO_ID

/**
 * Log all warnings.
 */
trait WarningLoggingCallback extends ExecutionCallback {

  def logWarn(line: String)

  def onWarning(warning: java.sql.SQLWarning, stm: Option[Symbol]) {
    stm match {
      case Some(id) if id != NO_ID =>
        logWarn("Statement '%s': [%s] %s".format(id.name, warning.getSQLState, warning.getMessage))
      case _ => logWarn("[%s] %s".format(warning.getSQLState, warning.getMessage))
    }
  }

}

trait SQLLoggingCallback extends ExecutionCallback {
  def logSQL(line: String)
  protected final def idString(id: Symbol): String = if (id == org.orbroker.NO_ID) "" else s" '${id.name}'"
}

/**
 * Log both before and after execution of all SQL statements.
 * This is recommended during development, particularly if
 * using dynamic SQL, because the SQL is logged before
 * execution is attempted, showing both invalid or long
 * running statements.
 */
trait FullLoggingCallback extends SQLLoggingCallback {

  import LoggingCallback._

  def beforeExecute(id: Symbol, sql: String) {
    logSQL(s"Executing${idString(id)}: ${sql.trim}")
  }

  def afterExecute(id: Symbol, sql: String, parms: Seq[Any], executionTime: Int) {
    val millis = executionTime / 1000
    logSQL(s"Finished${idString(id)} in $millis ms")
  }

  def beforeBatchExecute(id: Symbol, sql: String) {
    logSQL(s"Executing${idString(id)}: ${sql.trim}")
  }

  def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int) {
    if (parms.isEmpty) {
      logSQL(s"Cancelled${idString(id)}. No batch parameters provided.")
    } else {
      val millis = executionTime / 1000
      logSQL(s"Finished${idString(id)} (batch of ${parms.size}) in $millis ms")
    }
  }

}

/**
 * Log SQL after execution along with execution time in
 * milliseconds.
 */
trait AfterExecLoggingCallback extends SQLLoggingCallback {

  /** Log when empty batch execution was cancelled? */
  var logEmptyBatch = false

  import LoggingCallback._

  def beforeExecute(id: Symbol, sql: String) {}
  def beforeBatchExecute(id: Symbol, sql: String) {}

  def afterExecute(id: Symbol, sql: String, parms: Seq[Any], executionTime: Int) {
    val millis = executionTime / 1000
    val sqlWithValues = embedValues(sql.trim, parms)
    logSQL(s"Executed${idString(id)} in $millis ms: $sqlWithValues")
  }

  def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int) {
    if (!parms.isEmpty) {
      val millis = executionTime / 1000
      logSQL(s"Executed${idString(id)} (${parms.size} times) in $millis ms: ${sql.trim}")
    } else if (logEmptyBatch) {
      logSQL(s"Cancelled${idString(id)}. No batch parameters provided.")
    }
  }

}

object LoggingCallback {

  private val ReplaceParmHolders = """(\?)(?=(?:[^']|'[^']*')*$)""".r.pattern

  def embedValues(sql: String, parms: Seq[Any]): String = {
    val returnSql = new StringBuffer()
    val matcher = ReplaceParmHolders.matcher(sql)
    val i = parms.iterator
    while (matcher.find() && i.hasNext) {
      val sqlParm = i.next match {
        case null => "NULL"
        case parm @ (_: CharSequence | _: java.util.Date) => "'" + parm + "'"
        case parm => parm.toString
      }
      matcher.appendReplacement(returnSql, java.util.regex.Matcher.quoteReplacement(sqlParm))
    }
    matcher.appendTail(returnSql)
    returnSql.toString()
  }

}
