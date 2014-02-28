package org.orbroker.callback

import java.util.logging._

/**
 * Callback for logging to the JDK logger.
 * @param logger The JDK logger
 * @param sqlLevel The log level for executed SQL statements
 * @param thresholdMillis Only log statements taking longer than threshold
 * @param ignoreWarnings Set of SQL state codes to ignore when warned
 */
abstract class JDKLoggingCallback(logger: Logger, sqlLevel: Level)
    extends ExecutionCallback
    with WarningLoggingCallback
    with SQLLoggingCallback {

  def logWarn(line: String) = logger.log(Level.WARNING, line)
  
  def logSQL(line: String) = logger.log(sqlLevel, line)

}
