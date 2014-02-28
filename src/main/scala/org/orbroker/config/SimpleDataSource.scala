package org.orbroker.config

import java.io.PrintWriter
import java.sql.{Connection, Driver, DriverManager}
import javax.sql.DataSource

/**
 * Basic, non-pooling, data source.
 * @param url The database URL
 */
class SimpleDataSource(url: String) extends DataSource {

  def this(url: String, driverClass: String) {
    this(url)
    Class.forName(driverClass)
  }
  
  def this(url: String, driver: Driver) {
    this(url)
    DriverManager.registerDriver(driver)
  }
  
  def this(url: String, driverClass: Class[_ <: Driver]) {
    this(url, driverClass.newInstance)
  }
  
  def getParentLogger() = null

  def getConnection() = DriverManager.getConnection(url)

  def getConnection(username: String, password: String) = DriverManager.getConnection(url, username, password)

  def getLogWriter() = DriverManager.getLogWriter

  def setLogWriter(out: PrintWriter) = DriverManager.setLogWriter(out)

  def setLoginTimeout(seconds: Int) = DriverManager.setLoginTimeout(seconds)

  def getLoginTimeout() = DriverManager.getLoginTimeout

  def isWrapperFor(interface: Class[_]) = false
  def unwrap[T](interface: Class[T]) = throw new java.sql.SQLException("Cannot unwrap " + interface)

}