package org.orbroker.config

import org.orbroker._
import exception._
import callback.ExecutionCallback
import java.io.{ File, FilenameFilter, FileReader, Reader }
import javax.sql.{ DataSource }
import scala.collection._
import scala.collection.JavaConversions._

object ClasspathRegistrant {

  def apply(idToResource: Map[Symbol, String]): Registrant =
    new ClasspathRegistrant(idToResource, getClass)

  def apply(idToResource: Map[Symbol, String], cl: Class[_]): Registrant =
    new ClasspathRegistrant(idToResource, cl)

  def apply(
    resourcePath: String,
    ids: Set[Symbol],
    idToName: Symbol => String = { id: Symbol => id.name concat ".sql" },
    cl: Class[_] = getClass): Registrant = apply(toMap(resourcePath, ids, idToName), cl)

  private def toMap(p: String, ids: Set[Symbol], idToName: Symbol => String) = {
    val path = if (p endsWith "/") p else p concat "/"
    var map = immutable.Map.empty[Symbol, String]
    ids foreach { id =>
      map += id -> (path + idToName(id))
    }
    map
  }
}

/**
 * Load and register SQL resources from classpath.
 * @param ids The SQL statement ids
 * @param idToResource Function producing the absolute resource name from an id
 */
private class ClasspathRegistrant(
    idToResource: Map[Symbol, String],
    cl: Class[_]) extends Registrant {

  override def register(bb: BrokerConfig) {
    idToResource foreach {
      case (id, resource) =>
        cl.getResourceAsStream(resource) match {
          case null => {
            throw new ConfigurationException(
              "SQL statement '%s' not found: %s".format(id.name, resource))
          }
          case stream => bb.register(id, new java.io.InputStreamReader(stream))
        }
    }
  }

}
