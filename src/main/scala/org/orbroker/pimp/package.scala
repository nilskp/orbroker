package org.orbroker

/** Package for pimping O/R Broker. */
package object pimp {
  implicit def pimpQuery(qry: Queryable) = new PimpedQuery(qry)
  implicit def pimpOutParms(out: OutParms) = new PimpedOutParms(out)
}