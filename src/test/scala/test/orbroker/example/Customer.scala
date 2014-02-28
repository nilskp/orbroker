package test.orbroker.example

import org.orbroker._
import scala.collection.immutable.Map

class Customer(val name: String) {
  var id: Option[Int] = None
  var orders: Map[Int,Order] = Map.empty
} 

object CustomerExtractor extends RowExtractor[Customer] with JoinExtractor[Customer] {
  
  def extract(row: Row) = {
    val name = row.string("name").get
    val cust = new Customer(name)
    cust.id = row.integer("ID")
    cust
  }
  
  val key = Set("id")

  def extract(row: Row, join: Join) = extract(row)
  
}
