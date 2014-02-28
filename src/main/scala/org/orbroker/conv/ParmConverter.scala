package org.orbroker.conv

/**
 * Converter used for automatic conversion of
 * any reference type to a JDBC type.
 */
trait ParmConverter {

  /**
   * The type converting from
   */
  type T <: AnyRef

  /**
   * Defines the type that this converter converts from.
   * @return The class to convert from 
   */
  val fromType: Class[T]
  
  /**
   * Convert to a type understood by the JDBC implementation.
   * @param The parameter to convert
   * @return The converted parameter, something understood by JDBC
   */
  def toJdbcType(t: T): Any
  
}
