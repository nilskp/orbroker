package org.orbroker.adapt

import java.sql.Statement.{ RETURN_GENERATED_KEYS, NO_GENERATED_KEYS }
import java.sql.{Connection,PreparedStatement}

trait StatementAdapter {
  def prepareUpdate(id: Symbol, conn: Connection, sql: String, expectGeneratedKeys: Boolean): PreparedStatement
}

trait DefaultStatementAdapter extends StatementAdapter {
  def prepareUpdate(id: Symbol, conn: Connection, sql: String, expectGeneratedKeys: Boolean) = {
    val genKeyFlag = if (expectGeneratedKeys) RETURN_GENERATED_KEYS else NO_GENERATED_KEYS
    conn.prepareStatement(sql, genKeyFlag)
  }
}