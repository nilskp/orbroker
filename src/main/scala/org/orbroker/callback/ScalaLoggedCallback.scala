package org.orbroker.callback

/**
 * Callback for logging to [[scala.util.logging.Logged]].
 */
abstract class ScalaLoggedCallback(logger: scala.util.logging.Logged)
    extends ExecutionCallback
    with WarningLoggingCallback
    with SQLLoggingCallback {

  def logWarn(line: String) = logger.log(line)
  
  def logSQL(line: String) = logger.log(line)
  
}
