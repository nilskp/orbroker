package org.orbroker
import org.orbroker.callback.ExecutionCallback
import java.sql.{ SQLWarning, SQLException, Connection }

private[orbroker] trait JdbcType[-T] {
  def close(t: T)
  def getWarnings(t: T): SQLWarning
}

private[orbroker] object JdbcCloser {
  implicit object JdbcConnection extends JdbcType[java.sql.Connection] {
    def close(conn: java.sql.Connection) = conn.close()
    def getWarnings(conn: java.sql.Connection) = conn.getWarnings
  }
  implicit object JdbcResultSet extends JdbcType[java.sql.ResultSet] {
    def close(rs: java.sql.ResultSet) = rs.close()
    def getWarnings(rs: java.sql.ResultSet) = rs.getWarnings
  }
  implicit object JdbcStatement extends JdbcType[java.sql.Statement] {
    def close(ps: java.sql.Statement) = ps.close()
    def getWarnings(ps: java.sql.Statement) = ps.getWarnings
  }
  
  class JdbcCloseable[T](candidate: T, exe: JdbcType[T], callback: ExecutionCallback) {
    def checkAndClose() = checkWarningsAndClose(candidate, callback, exe, None)
    def checkAndClose(id: Symbol) = checkWarningsAndClose(candidate, callback, exe, Some(id))
  }

  implicit def toCloseable[T](candidate: T)(implicit exe: JdbcType[T], callback: ExecutionCallback) = new JdbcCloseable(candidate, exe, callback)

  private def checkWarningsAndClose[T](jdbc: T, callback: ExecutionCallback, exe: JdbcType[T], stm: Option[Symbol]) {
    try {
      handleWarning(stm, callback, exe.getWarnings(jdbc))
    } catch {
      case e: SQLException => // Ignore
    } finally {
      exe.close(jdbc)
    }
  }

  private def handleWarning(stm: Option[Symbol], callback: ExecutionCallback, warning: java.sql.SQLWarning) {
    if (warning != null) {
      callback.onWarning(warning, stm)
      handleWarning(stm, callback, warning.getNextWarning)
    }
  }

}