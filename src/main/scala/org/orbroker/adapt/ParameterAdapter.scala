package org.orbroker.adapt

import org.orbroker.exception._
import java.sql.{ PreparedStatement, CallableStatement, SQLException }

trait ParameterAdapter {
  /**
   * Set all parameter values at the appropriate index on the prepared statement.
   * Remember that the supplied sequence has base 0 and JDBC parameters has base 1.
   * @return Sequence of values for [[org.orbroker.callback.ExecutionCallback]], or empty to ignore
   */
  def setParameters(id: Symbol, ps: PreparedStatement, parmNames: IndexedSeq[String], valuesByName: String => Any): Seq[Any]
  /**
   * Set all parameter values at the appropriate index on the prepared statement and
   * remember to register OUT and INOUT parameters.
   * Remember that the supplied sequence has base 0 and JDBC parameters has base 1.
   * @return Sequence of values for [[org.orbroker.callback.ExecutionCallback]], or empty to ignore
   */
  def setParameters(id: Symbol, cs: CallableStatement, parmNames: IndexedSeq[String], valuesByName: String => Any): Seq[Any]
}

trait AbstractDefaultParameterAdapter extends ParameterAdapter {

  // Slight hack to get around https://issues.scala-lang.org/browse/SI-1938
  private val metadataAdapter: MetadataAdapter = new DefaultMetadataAdapter with CachingMetadataAdapter
  protected def metadata = metadataAdapter

  /**
   * Set parameter.
   * @param id The statement id
   * @param ps Prepared statement
   * @param parm The parameter value
   * @param parmIdx The JDBC parameter index
   */
  def setParameter(id: Symbol, ps: PreparedStatement, parm: Any, parmIdx: Int)

  def setParameters(id: Symbol, ps: PreparedStatement, parmNames: IndexedSeq[String], valuesByName: String => Any): Seq[Any] = {
    val values = new Array[Any](parmNames.length)
    var idx = 0
    while (idx < parmNames.length) {
      val name = parmNames(idx)
      values(idx) = setParm(id, ps, name, valuesByName(name), idx + 1)
      idx += 1
    }
    values
  }

  def setParameters(id: Symbol, cs: CallableStatement, parmNames: IndexedSeq[String], valuesByName: String => Any): Seq[Any] = {
    val values = new Array[Any](parmNames.length)
    val callInfo = metadata.procedureMetadata(id, parmNames, cs)
    var idx = 0
    while (idx < parmNames.length) {
      val parmIdx = idx + 1
      val name = parmNames(idx)
      if (callInfo.isOutParm(parmIdx)) {
        registerOutParm(callInfo, cs, parmIdx, name)
      }
      values(idx) = if (callInfo.isInParm(parmIdx)) {
        setParm(id, cs, name, valuesByName(name), parmIdx)
      } else {
        new { override def toString = "?" }
      }
      idx = parmIdx
    }
    values
  }

  protected def setParm(id: Symbol, ps: PreparedStatement, name: String, value: Any, idx: Int): Any = {
    try {
      setParameter(id, ps, value, idx)
      value
    } catch {
      case e: Exception => throw newConfigException(id, name, value.asInstanceOf[AnyRef], e)
    }
  }

  protected def newConfigException(id: Symbol, parmName: String, value: AnyRef, e: Exception): ConfigurationException = {
    val (parmType, parmValue) = if (value == null) {
      ("<unknown>", "NULL")
    } else {
      (value.getClass.getName, value.toString)
    }
    val msg = "Statement '%s' could not set parameter :%s with value %s and type '%s'".format(id.name, parmName, parmValue, parmType)
    new org.orbroker.exception.ConfigurationException(msg, e)
  }

  protected def registerOutParm(callInfo: ProcedureMetadata, cs: CallableStatement, parmIdx: Int, parmName: String) =
    try {
      val pType = callInfo.parmType(parmIdx)
      cs.registerOutParameter(parmIdx, pType)
    } catch {
      case e: SQLException =>
        throw new ConfigurationException(
          "Statement '%s' failed to register OUT/INOUT parameter :%s".
            format(callInfo.id.name, parmName), e)
    }

}

/**
 * The default parameter adapter.
 */
trait DefaultParameterAdapter extends AbstractDefaultParameterAdapter {
  def setParameter(id: Symbol, ps: PreparedStatement, parm: Any, parmIdx: Int) =
    ps.setObject(parmIdx, parm)
}

/**
 * General adapter for drivers that does not approve
 * of passing `null` to [[java.sql.PreparedStatement#setObject(Int,java.lang.Object)]].
 */
trait NullParmAdapter extends DefaultParameterAdapter {

  override def setParameter(id: Symbol, ps: PreparedStatement, parm: Any, parmIdx: Int) {
    if (parm == null) {
      ps.setNull(parmIdx, java.sql.Types.VARCHAR)
    } else {
      super.setParameter(id, ps, parm, parmIdx)
    }
  }

}

/**
 * General adapter for drivers that does not approve
 * of passing `null` to [[java.sql.PreparedStatement#setObject(Int,java.lang.Object)]],
 * but instead uses parameter metadata to resolve the exact column type.
 */
trait TypedNullParmAdapter extends DefaultParameterAdapter {

  override def setParameter(id: Symbol, ps: PreparedStatement, parm: Any, parmIdx: Int) {
    if (parm == null) {
      val parmType = ps.getParameterMetaData.getParameterType(parmIdx)
      ps.setNull(parmIdx, parmType)
    } else {
      super.setParameter(id, ps, parm, parmIdx)
    }
  }

}
