package org.orbroker.config

import org.orbroker._
import org.orbroker.callback.ExecutionCallback
import java.io.{ File, FilenameFilter, FileReader, Reader }
import javax.sql.{ DataSource }
import scala.collection.mutable.HashMap

object FileSystemRegistrant {
  /**
   * Assume SQL files have extension `.sql` and that
   * the name is the statement id.
   * @param ds Data source
   * @param dir SQL file directory
   */
  def apply(dir: File): Registrant = apply(dir, SQLFileFilter, NameIsIDExtractor)
  
  def apply(dir: File, filter: FilenameFilter, idExtractor: String ⇒ Symbol): Registrant =
    new FileSystemRegistrant(dir, filter, idExtractor)
}

/**
 * Registrant for SQL files in the file system.
 * @param dir Directory containing files
 * @param filter Filter for files in directory
 * @param idExtractor Extractor for SQL statement ID in file name
 */
private class FileSystemRegistrant (
    dir: File, filter: FilenameFilter, 
    idExtractor: String ⇒ Symbol
) extends Registrant {

  require(dir.isDirectory, dir + " is not a directory")

  override def register(bb: BrokerConfig) { 
    for (file ← dir. listFiles(filter)) {
      val id = idExtractor(file.getName)
      val sql = new FileReader(file)
      bb.register(id, sql)
    }
  }

}

/**
 * Simple file filter that accepts files ending in `.sql`
 */
private[config]object SQLFileFilter extends FilenameFilter {
  def accept(dir: File, name: String) = name.toLowerCase.endsWith(".sql")
}
