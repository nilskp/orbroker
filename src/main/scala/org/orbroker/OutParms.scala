package org.orbroker

import java.sql.CallableStatement

import org.orbroker.adapt.BrokerAdapter
import org.orbroker.callback.ExecutionCallback

/**
 * Representation of output parameters. Names
 * will match whatever the parameter was named
 * in the SQL statement, verbatim.
 * All values are  wrapped in `Option` to represent nullability.
 */
trait OutParms extends ResultSetProducer {
  def apply(parmName: String)(implicit tz: java.util.TimeZone = null): OutParm
}

private[orbroker] class OutParmsImpl(
    val id: Symbol,
    parmIdxMap: Map[String, Int],
    cs: CallableStatement,
    val callback: ExecutionCallback,
    val adapter: BrokerAdapter) extends OutParms {

  def apply(parmName: String)(implicit tz: java.util.TimeZone = null) = new OutParm(parmName, cs, tz, parmIdxMap)

}
