package test.orbroker.example

import scala.math.{ BigDecimal => Decimal }
import scala.math.BigDecimal.RoundingMode._
import org.orbroker._
import org.orbroker.pimp._

class Item(val name: String, private var _price: Decimal) {
  price = _price // Round
  def price = _price
  def price_=(prc: Decimal) { _price = prc.setScale(2, HALF_UP) }
  var id: Option[Int] = None
}

class ItemWithOrders(n: String, p: Decimal, orders: Set[Order]) extends Item(n, p)

object ItemExtractor extends RowExtractor[Item] with JoinExtractor[Item] {
  override def extract(row: Row) = {
    val item = new Item(row("Name").as[String], row("price").as[Decimal])
    item.id = row("ID").opt[Int]
    item
  }

  val key = Set("ID")

  override def extract(row: Row, join: Join) = extract(row)

}

class CrazyItemExtractor(orderToken: Token[Order])(implicit session: QuerySession) extends RowExtractor[ItemWithOrders] with JoinExtractor[ItemWithOrders] {
  val itemCount2 = session.selectOne[Int]("SELECT COUNT(*) FROM Item").get
  val itemCount = session.selectOne[Int]('countItems).get

  override def extract(row: Row) = {
    val itemID = row("ID").opt[Int]
    //    var orderSet: Set[Order] = Set.empty
    val orderSet = session.select(orderToken) { orders =>
      orders.filter(_.items.exists(_.id == itemID)).toSet
    }
    val item = new ItemWithOrders(row("Name").as[String], row("price").as[Decimal], orderSet)
    item.id = itemID
    item
  }

  val key = Set("ID")
  override def extract(row: Row, join: Join) = extract(row)

}
