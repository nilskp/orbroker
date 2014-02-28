package org.orbroker.config

import org.orbroker._
import org.orbroker.callback.Deaf
import org.orbroker.callback.ExecutionCallback
import org.orbroker.adapt._
import org.orbroker.conv._
import org.orbroker.exception._
import org.orbroker.config.dynamic.DynamicSupport

import scala.collection.immutable.Map
import scala.collection.Set

import java.io.Reader

/**
 * Configuration for [[org.orbroker.Broker]].
 */
class BrokerConfig(dataSource: javax.sql.DataSource) extends DynamicSupport {

  /**
   * The default transaction isolation level. If not set, will
   * use whatever data source provides as default.
   * Standard values:<ul>
   * <li> [[java.sql.Connection#TRANSACTION_NONE]]
   * <li> [[java.sql.Connection#TRANSACTION_READ_UNCOMMITTED]]
   * <li> [[java.sql.Connection#TRANSACTION_READ_COMMITTED]]
   * <li> [[java.sql.Connection#TRANSACTION_REPEATABLE_READ]]
   * <li> [[java.sql.Connection#TRANSACTION_SERIALIZABLE]]
   * </ul>
   */
  var defaultIsolationLevel: Option[Int] = None
  def defaultIsolationLevel_=(level: Int): Unit = defaultIsolationLevel = Some(level)
  /** Default statement execution timeout in seconds. 0 means no timeout. */
  var defaultTimeout = 0
  /** Default fetch size hint. 0 means no hint. */
  var defaultFetchSize = 0
  /** Notifying callback. */
  var callback: ExecutionCallback = Deaf
  /** Compatibility adapter. */
  var adapter: BrokerAdapter = DefaultAdapter
  /** Optional catalog name. */
  var catalog: String = null
  /**
   * Always use prepared statements.
   * If `true` will always use prepared statements, even when
   * statement has no parameters.
   */
  var alwaysPrepare = true
  /** Trim SQL? If true, will remove excessive whitespace and comments */
  var trimSQL = true

  import org.orbroker.SQLStatement._

  private var defaultParms: Map[String, Any] = Map.empty

  private var usernamePassword: Option[(String, String)] = None

  /**
   * Add default parameter. Default parameters are always available to a running SQL statement,
   * but can be overridden on a per-call basis.
   * They are mostly useful for dynamic SQL generation for specifying things like the schema, etc.
   */
  def addDefaultParm(name: String, value: Any) = defaultParms += name -> value
  def addDefaultParms(parms: Map[String, Any]) = defaultParms ++= parms

  /**
   * Set username and password, if different from what's used
   * by data source.
   */
  def setUser(username: String, password: String) = usernamePassword = Some((username, password))

  /**
   * Verify that expected statements have been registered to this builder.
   * @param expected The set of expected statement ids
   * @return Set of statements registered that was not in the set of expected statements
   * @throws MissingStatementException when verification fails
   */
  @throws(classOf[MissingStatementException])
  def verify(expected: Set[Symbol]): Set[Symbol] = {
    val missing = expected -- registry.keySet
    if (!missing.isEmpty) {
      throw new MissingStatementException(missing)
    }
    registry.keySet -- expected
  }

  implicit private def toConverterMap(convs: Seq[ParmConverter]) = {
    var map: Map[Class[_], ParmConverter] = Map.empty
    for (conv ← convs) map += (conv.fromType -> conv)
    map
  }

  private var registry: Map[Symbol, Seq[String]] = Map.empty

  def register(id: Symbol, sqlStatement: Reader) {
    val sql = new scala.collection.mutable.ArrayBuffer[String]
    sqlStatement foreach { sql += _ }
    registry += id -> sql
  }

  override protected def dynamicStatement(id: Symbol, sql: String, sqlLines: Seq[String]): Option[SQLStatement] = None

  private def createStatement(id: Symbol, sqlLines: Seq[String]): SQLStatement = {
    val sql = sqlLines.mkString(" ")
    if (isProcedureCall(sql)) {
      new StaticStatement(id, sqlLines, trimSQL, callback, adapter) with CallStatement
    } else dynamicStatement(id, sql, sqlLines) match {
      case Some(dynStm) ⇒ dynStm
      case None ⇒ new StaticStatement(id, sqlLines, trimSQL, callback, adapter) with ModifyStatement with QueryStatement
    }
  }

  /**
   * Build the broker.
   * @return A new broker instance
   */
  private[orbroker] def build(): Broker = {
    var statements: Map[Symbol, SQLStatement] = Map.empty
    for ((id, sql) ← registry) {
      val stm = createStatement(id, sql)
      statements += id -> stm
    }
    new Broker(callback, dataSource,
      defaultIsolationLevel, defaultTimeout, defaultFetchSize, alwaysPrepare, trimSQL,
      usernamePassword, Option(catalog), statements, adapter, defaultParms)
  }
}
