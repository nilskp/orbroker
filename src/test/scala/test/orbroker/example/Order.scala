package test.orbroker.example

import org.orbroker._
import scala.collection.mutable.ArrayBuffer
import org.joda.time.LocalDate

class Order(val date: LocalDate, val customer: Customer, val items: Seq[Item]) {
  var id: Option[Int] = None
}

object OrderExtractor extends JoinExtractor[Order] {

  private val custAlias = Map("id" -> "CustomerID", "Name" -> "CustomerName")
  private val itemAlias = Map("id" -> "ItemID", "Name" -> "ItemName")

  val key = Set("ID")

  def extract(row: Row, join: Join) = {
    val orderId = row.integer("ID")
    val date = new LocalDate(row.date("OrderDate").get.getTime)
    val cust = join.extractOne(CustomerExtractor, custAlias).get
    val items = new ArrayBuffer[Item]
    join.extractGroup(ItemExtractor, itemAlias) { item ⇒
      items += item
    }
    val order = new Order(date, cust, items)
    order.id = orderId
    cust.orders += order.id.get -> order
    order
  }
}
