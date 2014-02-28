package org.orbroker.callback

/**
 * Log all warnings.
 */
trait WarningLoggingCallback extends ExecutionCallback {

  def logWarn(line: String)

  def onWarning(warning: java.sql.SQLWarning, stm: Option[Symbol]) {
    stm match {
      case Some(id) ⇒ {
        logWarn("Statement '%s': [%s] %s".format(id.name, warning.getSQLState, warning.getMessage))
      }
      case None ⇒ logWarn("[%s] %s".format(warning.getSQLState, warning.getMessage))
    }
  }

}

trait SQLLoggingCallback extends ExecutionCallback {
  def logSQL(line: String)
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
    logSQL(s"Executing '${id.name}': ${sql.trim}")
  }

  def afterExecute(id: Symbol, sql: String, parms: Seq[Any], executionTime: Int) {
    val millis = executionTime / 1000
    logSQL("Finished '%s' in %,d ms".format(id.name, millis))
  }

  def beforeBatchExecute(id: Symbol, sql: String) {
    logSQL("Executing '%s': %s".format(id.name, sql.trim))
  }

  def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int) {
    if (parms.isEmpty) {
      logSQL("Cancelled '%s'. No batch parameters provided.".format(id.name))
    } else {
      val millis = executionTime / 1000
      logSQL("Finished '%s' (batch of %,d) in %,d ms".format(id.name, parms.size, millis))
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
    logSQL("Executed '%s' in %,d ms: %s".format(id.name, millis, sqlWithValues))
  }

  def afterBatchExecute(id: Symbol, sql: String, parms: Seq[Seq[Any]], executionTime: Int) {
    if (!parms.isEmpty) {
      val millis = executionTime / 1000
      logSQL("Executed '%s' (%,d times) in %,d ms: %s".format(id.name, parms.size, millis, sql.trim))
    } else if (logEmptyBatch) {
      logSQL("Cancelled '%s'. No batch parameters provided.".format(id.name))
    }
  }

}

object LoggingCallback {

  private val ReplaceParmHolders = java.util.regex.Pattern.compile("""(\?)(?=(?:[^']|'[^']*')*$)""")

  def embedValues(sql: String, parms: Seq[Any]): String = {
    val returnSql = new StringBuffer()
    val matcher = ReplaceParmHolders.matcher(sql)
    val i = parms.iterator
    while (matcher.find() && i.hasNext) {
      val sqlParm = i.next match {
        case null ⇒ "NULL"
        case parm @ (_: CharSequence | _: java.util.Date) ⇒ "'" + parm + "'"
        case parm ⇒ parm.toString
      }
      matcher.appendReplacement(returnSql, java.util.regex.Matcher.quoteReplacement(sqlParm))
    }
    matcher.appendTail(returnSql)
    returnSql.toString()
  }

}