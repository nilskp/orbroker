package org.orbroker.adapt

import java.sql.CallableStatement

trait MetadataAdapter {
  def procedureMetadata(id: Symbol, parms: IndexedSeq[String], cs: CallableStatement): ProcedureMetadata
}

trait ProcedureMetadata {
  def id: Symbol
  /** @param parmIdx The JDBC parameter index, one-based. */
  def isInParm(parmIdx: Int): Boolean
  /** @param parmIdx The JDBC parameter index, one-based. */
  def isOutParm(parmIdx: Int): Boolean
  /** @param parmIdx The JDBC parameter index, one-based. */
  def parmType(parmIdx: Int): Int
}

class DefaultProcedureMetadata(
    val id: Symbol,
    inParm: Array[Boolean],
    outParm: Array[Boolean],
    parmTypes: Array[Int]) extends ProcedureMetadata {
  def isInParm(parmIdx: Int) = inParm(parmIdx - 1)
  def isOutParm(parmIdx: Int) = outParm(parmIdx - 1)
  def parmType(parmIdx: Int) = parmTypes(parmIdx - 1)
}

trait CachingMetadataAdapter extends DefaultMetadataAdapter {
  import scala.collection.mutable.HashMap
  private val procMetadataMap = new HashMap[Symbol, ProcedureMetadata]

  abstract override def procedureMetadata(id: Symbol, parms: IndexedSeq[String], cs: CallableStatement): ProcedureMetadata = {
    procMetadataMap.synchronized {
      procMetadataMap.getOrElseUpdate(id, super.procedureMetadata(id, parms, cs))
    }
  }
}

trait DefaultMetadataAdapter extends MetadataAdapter {
  def procedureMetadata(id: Symbol, parms: IndexedSeq[String], cs: CallableStatement): ProcedureMetadata = {
    import java.sql.ParameterMetaData._
    val metaData = cs.getParameterMetaData()
    val parmCount = metaData.getParameterCount
    val parmTypes = new Array[Int](parmCount)
    val isInParms, isOutParms = new Array[Boolean](parmCount)
    var idx = 0
    while (idx < parmCount) {
      parmTypes(idx) = metaData.getParameterType(idx + 1)
      val mode = metaData.getParameterMode(idx + 1)
      isInParms(idx) = mode != parameterModeOut
      isOutParms(idx) = mode == parameterModeOut || mode == parameterModeInOut
      idx += 1
    }
    new DefaultProcedureMetadata(id, isInParms, isOutParms, parmTypes)
  }
}