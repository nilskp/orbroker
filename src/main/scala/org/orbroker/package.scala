package org

/** Main package for O/R Broker. */
package object orbroker {

  import java.io.{ Reader, LineNumberReader }
  private[orbroker] implicit def readerToIterator(reader: Reader): Iterator[String] = {
    val lnr =
      if (reader.isInstanceOf[LineNumberReader])
        reader.asInstanceOf[LineNumberReader]
      else
        new LineNumberReader(reader)
    new Iterator[String] {
      var n: String = lnr.readLine
      override def hasNext = {
        n != null
      }
      override def next = {
        val nn = n
        n = lnr.readLine
        nn
      }
    }
  }

  private[orbroker] val NO_ID = Symbol("<noname>")

  implicit def stringToReader(string: String) = new java.io.StringReader(string)
  implicit def sym2Token[T](id: Symbol): Token[T] = Token(id)
  implicit def sql2Token[T](sql: String): Token[T] = Token(sql, NO_ID)

  import scala.collection.{ Traversable, TraversableView, Iterator }
  import scala.collection.generic.FilterMonadic

  implicit def viewToTraversable[A](view: TraversableView[A, Traversable[A]]) = new Traversable[A] {
    def foreach[U](callback: (A) ⇒ U) = view.foreach(callback)
  }

  implicit def filterMonadicToTraversable[A](monadic: FilterMonadic[A, Traversable[A]]) = new Traversable[A] {
    def foreach[U](callback: (A) ⇒ U) = monadic.foreach(callback)
  }

  implicit def iteratorToTraversable[A](iterator: Iterator[A]) = new Traversable[A] {
    def foreach[U](callback: (A) ⇒ U) {
      while (iterator.hasNext) callback(iterator.next)
    }
  }
  
  implicit def stringToDataSource(url: String) = new config.SimpleDataSource(url)

}