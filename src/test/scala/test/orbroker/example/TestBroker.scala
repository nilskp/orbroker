package test.orbroker.example

import org.junit._
import org.junit.Assert._
import org.orbroker._
import org.orbroker.config.dynamic._
import org.orbroker.exception._
import org.orbroker.config._
import org.orbroker.adapt._
import org.orbroker.callback._
import org.orbroker.conv._
import org.orbroker.pimp._
import org.orbroker.util._
import org.apache.derby.jdbc._
import scala.collection.mutable.ListBuffer

import org.joda.time.LocalDate
import java.io._
import java.util.logging._
import java.sql._

class TestBroker {

  import TestBroker._

  val FullLongName = "ReallyLongFirstName AndLongLastName"
  val LongNameLimited = FullLongName.substring(0, 30)

  val InsertOrderItem = Token('insertOrderItem)
  val SelectCustomer = Token('selectCustomer, CustomerExtractor)
  val InsertCustomer = Token('insertCustomer, IntColumn)
  val SelectOrders = Token('selectOrders, OrderExtractor)
  val InsertOrder = Token('insertOrder, JodaLocalDateConv)
  val InsertItem = Token('insertItem, IntColumn, BigDecimalConv)
  val CallKongKat = Token('callKongKat)
  val CountItems = Token[Int]('countItems)

  private val dbName = "ORBrokerTest"
  private val url = "jdbc:derby:" + dbName + ";create=true"
  private var ds: javax.sql.DataSource = null
  private var config: BrokerConfig = null

  private def create(stm: Statement, create: String) {
    try {
      stm.executeUpdate("create " + create)
    } catch {
      case _ ⇒ // Ignore
    }
  }

  private def testJSON(broker: Broker) {
    object ItemsExtractor extends RowExtractor[Unit] {
      val out = new java.io.StringWriter
      def writeKeyValue(key: String, value: Any) {
        out.append('"')
        out.write(key)
        out.append('"')
        out.append(':')
        value match {
          case n @ (_: Number | _: Boolean) ⇒
            out.write(n.toString)
          case r ⇒
            out.append('"')
            out.write(String.valueOf(r))
            out.append('"')
        }
        out.append(',')
      }
      def extract(row: Row) {
        out.append('{')
        writeKeyValue("id", row.integer("ID").get)
        writeKeyValue("name", row.string("Name").get)
        writeKeyValue("price", row.decimal("Price").get)
        out.append('}').append(',')
      }
    }
    ItemsExtractor.out.append('[')
    broker.readOnly() { session ⇒
      val t = Token('selectItems, ItemsExtractor)
      session.selectInto(t)
    }
    ItemsExtractor.out.append(']')
    println("JSON: " + ItemsExtractor.out.getBuffer.toString)

  }

  @Before
  def setup() {
    System.getProperties.setProperty("derby.system.home", "target/test-databases")
    Class.forName("org.apache.derby.jdbc.AutoloadedDriver")
    val conn = DriverManager.getConnection(url);
    //    val dds = new EmbeddedSimpleDataSource()
    //    dds setDatabaseName dbName
    //    ds = dds
    ds = new org.orbroker.config.SimpleDataSource(url)
    config = new BrokerConfig(ds) with FreeMarkerSupport with VelocitySupport
    FileSystemRegistrant(sqlDir).register(config)
    val cpIds = Set('selectCustomer, 'selectItems, 'selectOrders, 'selectItems_velocity)
    ClasspathRegistrant("/sql", cpIds).register(config)
    config.alwaysPrepare = false
    val callback = new MultiProxyCallback(
      new JDKLoggingCallback(Logger, Level.INFO) with FullLoggingCallback with IgnoreWarnings {
        val ignoreSQLState = Set("01J01")
      },
      new ScalaLoggedCallback(new scala.util.logging.ConsoleLogger {}) with AfterExecLoggingCallback with IgnoreWarnings {
        val ignoreSQLState = Set("01J01")
      }
    )
    config.callback = callback
    config.adapter = DerbyAdapter
    var stm = conn.createStatement
    create(stm, "TABLE JustTheKey (ID integer primary key GENERATED ALWAYS AS IDENTITY)")
    create(stm, "TABLE CUSTOMER (ID integer primary key GENERATED ALWAYS AS IDENTITY (START WITH 1001, INCREMENT BY 1), name varchar(30) not null)")
    create(stm, "TABLE CustOrder (ID integer primary key GENERATED ALWAYS AS IDENTITY (START WITH 2001, INCREMENT BY 1), OrderDate date not null, CustomerID integer not null)")
    create(stm, "TABLE ITEM (ID integer primary key GENERATED ALWAYS AS IDENTITY (START WITH 3001, INCREMENT BY 1), name varchar(30) not null, price decimal(7,2) not null)")
    create(stm, "TABLE OrderItem (OrderID integer not null, ItemID integer not null, primary key (OrderID, ItemID))")
    create(stm, "TABLE Numbers (num integer)")
    create(stm,
      """ 
PROCEDURE KONGKAT (IN frst CHAR(1), INOUT scnd CHAR(1), OUT aut VARCHAR(10))
EXTERNAL NAME 'test.orbroker.example.sp.SPCall.kongKat'
LANGUAGE JAVA NO SQL PARAMETER STYLE JAVA
""")
    conn.close
  }

  @Test
  def autoCommit() {
    val broker = Broker(config)
    try {
      broker.transactional() { session ⇒
        session.execute('insertNumber, "number" -> 5000)
      }
      fail("Should fail missing commit")
    } catch {
      case e: RollbackException ⇒ // Expected
    }
    val count = broker.transaction() { session ⇒
      session.execute('insertNumber, "number" -> 5000)
    }
    assertEquals(1, count)
  }

  @Test
  def updateSequence() {
    import java.sql.Connection._

    val broker = Broker(config)
    val count = 17L
    val range = broker.transactional(TRANSACTION_REPEATABLE_READ) { session ⇒
      try {
        session.execute("CREATE TABLE SEQNUM (NEXTID BIGINT NOT NULL)")
        session.execute("INSERT INTO SEQNUM VALUES(0)")
        session.commit()
      } catch {
        case _: SQLException ⇒ session.rollback()
      }

      val first = session.selectOne[Long]("SELECT NEXTID FROM SEQNUM").get
      val newNext = first + count
      session.execute("UPDATE SEQNUM SET NextID = :nextID", "nextID" -> newNext)
      session.commit()

      first until newNext
    }
    println("Allocated sequence: " + range)
    assertEquals(count, range.size)
  }

  private def testSP(broker: Broker) {
    broker.readOnly() { qry ⇒
      var char2 = "z"
      var result: String = null
      qry.callForParms(CallKongKat, "char1" -> "a", "char2" -> char2) { parms ⇒
        char2 = parms.string("char2").get
        result = parms.string("result").get
      }
      assertEquals("Z", char2)
      assertEquals("a-z", result)
    }
  }

  private def testSP2(broker: Broker) {
    case class SPOut(char2: String, result: String)
    object OutExtractor extends OutParmExtractor[SPOut] {
      def extract(out: OutParms) = SPOut(out.string("char2").get, out.string("result").get)
    }
    val token = Token(CallKongKat.id, OutExtractor)
    broker.readOnly() { qry ⇒
      val char2 = "z"
      val spOut = qry.selectOne(token, "char1" -> "a", "char2" -> "z").get
      assertEquals("Z", spOut.char2)
      assertEquals("a-z", spOut.result)
    }
  }

  private def testSubQuery(broker: Broker) {
    broker.readOnly() { implicit session ⇒
      val ext = new CrazyItemExtractor(SelectOrders)
      assertEquals(ext.itemCount, ext.itemCount2)
      val items = session.selectAll(Token('selectItems, ext))
      assertFalse("No items", items.isEmpty)
      println("All items: " + items.mkString(", "))
      assertEquals(ext.itemCount, items.size)
    }
  }

  private def insertNoParmBatch(broker: Broker) {
    broker.transaction() { session ⇒
      val batchSize = 7
      val inserted = session.executeBatch("INSERT INTO JustTheKey VALUES DEFAULT", batchSize)
      assertEquals(batchSize, inserted)
    }
  }

  private def tooLongName(broker: Broker, conn: Connection): Customer = {
    broker.transactional(conn) { session ⇒
      val cust = new Customer(FullLongName)
      insertLongNameCustomer(session, cust)
    }
  }

  private def insertLongNameCustomer(session: Transaction, cust: Customer): Customer = {
    try {
      cust.id = session.executeForKey(InsertCustomer, "cust" -> cust)
      cust
    } catch {
      case e: SQLException if (e.getSQLState == "22001") ⇒ {
        insertLongNameCustomer(session, new Customer(cust.name.substring(0, cust.name.length - 1)))
      }
    }
  }

  @Test
  def exceptionAdapter {
    object TestAdapter extends adapt.DefaultExceptionAdapter {
      def testNullState = state(new SQLException)
    }
    TestAdapter.testNullState
  }

  private def testNullAndNoneAndWarnings(broker: Broker) {
    val numbers = Seq(null, 1, 2, 3, 4, 5, null, None, 7, 8, 9, None)
    val countAndAverage = broker.transactional() { session ⇒
      assertEquals(0, session.executeBatch('insertNumber, "number" -> Seq()))
      val count = session.executeBatch('insertNumber, "number" -> numbers)
      assertEquals(numbers.size, count)
      session.commit()
      session.selectOne(Token('getNumbersAverage)).get: (Int, Float)
    }
    assertEquals(numbers.size, countAndAverage._1)
    assertEquals(4.875f, countAndAverage._2, 0)
    Logger.info("Numbers %s has an average of %f".format(numbers, countAndAverage._2))
  }

  @Test
  def verify {
    val missingId = 'lkjasdfkja
    try {
      config.verify(Set(missingId))
      fail("Should fail on missing id")
    } catch {
      case e: MissingStatementException ⇒ assertTrue(missingId + " not in message: " + e.getMessage, e.getMessage.contains(missingId.toString))
    }

    val unexpected = config.verify(Set(InsertOrderItem.id, SelectCustomer.id, InsertCustomer.id,
      SelectOrders.id, InsertOrder.id, InsertItem.id, CallKongKat.id, CountItems.id))

    assertEquals(4, unexpected.size)
    assertTrue(unexpected.contains('getNumbersAverage))
    assertTrue(unexpected.contains('insertNumber))
  }

  @Test
  def indexedParm() {
    val broker = Broker(config)
    broker.transactional() { session ⇒
      session.execute("CREATE TABLE FOOBAR ( ID integer primary key, name varchar(10) not null )")
      type FooBar = (Int, String)
      val values = Seq((1, "Foo"), (2, "Bar"), (3, "Baz"), (4, "Fee"))
      assertEquals(values.size, session.executeBatch("INSERT INTO FOOBAR VALUES(:foo._1, :foo._2)", "foo" -> values))
      session.commit()
    }
    val names = broker.readOnly() { session ⇒
      val ids = Seq(1, 2)
      session.selectAll[String]("SELECT Name From FooBar WHERE ID IN (:ids[0], :ids[1]) ORDER BY Name", "ids" -> ids)
    }
    assertEquals(2, names.size)
    assertEquals("Bar", names(0))
    assertEquals("Foo", names(1))
  }

  @Test
  def test1() {

    val broker = Broker(config)

    testSP(broker)
    testSP2(broker)
    insertNoParmBatch(broker)

    val conn = ds.getConnection
    conn.setAutoCommit(false)
    val cust = {
      val cust = tooLongName(broker, conn)
      conn.commit()
      conn.close()
      cust
    }
    Logger.info("Too long name: \"%s\", got ID: %d".format(cust.name, cust.id.get))
    assertEquals(LongNameLimited, cust.name)
    testNullAndNoneAndWarnings(broker)

    val customer = new Customer("Nils")
    val bigMac = new Item("BigMac", 2.95)
    val fries = new Item("Fries", 1.35)
    val coke = new Item("Diet Coke", .99)
    val items = Seq(bigMac, fries, coke)
    val order = new Order(new LocalDate, customer, items)
    val shake = new Item("Strawberry Shake", 3.10)
    val items2 = Seq(fries, shake)
    val order2 = new Order(new LocalDate, customer, items2)

    broker.transactional() { session ⇒

      insertCustomer(session, customer)
      insertItems(session, items)
      insertItem(session, shake)
      insertOrders(session, Seq(order, order2))

      session.executeBatch(InsertOrderItem, "item" -> items, "order" -> order)
      session.executeBatch(InsertOrderItem, "item" -> items2, "order" -> order2)
      session.commit()
    }

    val cust2 = broker.readOnly() { session ⇒
      session.timeout = 1
      val cust = session.selectOne(SelectCustomer, "id" -> customer.id)
      Logger.info(cust.toString)
      val orders = new ListBuffer[Order]
      val count = session.selectToBuffer(SelectOrders, orders, "cust" -> customer)
      assertEquals(orders.size, count)
      for (order ← orders) {
        Logger.info(order.toString)
      }
      assertSame(orders(0).customer, orders(1).customer)
      val itemCount = session.selectOne(CountItems).get
      assertEquals(4, itemCount)
      cust.get
    }
    assertEquals(cust2.id, customer.id)
    testSubQuery(broker)
    testOnTheFlyQuery(broker)
    testJSON(broker)
  }

  private def testOnTheFlyQuery(broker: Broker) {
    broker.readOnly() { session ⇒
      val sql = "SELECT COUNT(*) FROM CUSTOMER"
      val onTheFly = Token[Int](sql, 'onTheFly)
      val count = session.selectOne(onTheFly).get
      Logger.info("There are %d customer(s)".format(count))
    }
  }

  private def insertCustomer(txn: Transaction, customer: Customer) {
    customer.id = txn.executeForKey(InsertCustomer, "cust" -> customer)
    Logger.info(customer + " got ID: " + customer.id.get)
  }

  private def insertOrders(txn: Transaction, orders: Seq[Order]) {
    val orderIter = orders.iterator
    // No key converter specified for "insertOrder",
    // so using the default object returned, which,
    // considering the column is defined as INTEGER,
    // strangely is BigDecimal,
    val savepoint = txn.makeSavepoint()
    try {
      txn.executeBatchForKeys(InsertOrder, "order" -> orders) { newId: java.math.BigDecimal ⇒
        val order = orderIter.next
        order.id = Some(newId.intValue)
        Logger.info(order + " got ID: " + order.id.get)
      }
    } catch {
      case e: RuntimeException if e.getMessage.contains("ASSERT FAILED") ⇒ {
        txn.rollbackSavepoint(savepoint)
        Logger.info("Getting all the generated keys from a batch INSERT is not supported by this driver")
        for (order ← orders)
          insertOrder(txn, order)
      }
      case e: UnsupportedJDBCOperationException ⇒ {
        txn.rollbackSavepoint(savepoint)
        Logger.info("Getting all the generated keys from a batch INSERT is not supported by this driver")
        for (order ← orders)
          insertOrder(txn, order)
      }
    }
  }

  private def insertOrder(txn: Transaction, order: Order) {
    txn.executeForKey(InsertOrder, "order" -> order) foreach { newId: java.math.BigDecimal ⇒
      order.id = Some(newId.intValue)
      Logger.info(order + " got ID: " + order.id.get)
    }
  }

  private def insertItems(txn: Transaction, items: Seq[Item]) {
    val itemsIter = items.iterator
    val savepoint = txn.makeSavepoint()
    try {
      txn.executeBatchForKeys(InsertItem, "item" -> items) { newId: Int ⇒
        val item = itemsIter.next
        item.id = Some(newId)
        Logger.info(item + " got ID: " + item.id.get)
      }
    } catch {
      // Normally, this exception should not be caught.
      case e: RuntimeException if e.getMessage.contains("ASSERT FAILED") ⇒ {
        txn.rollbackSavepoint(savepoint)
        Logger.info("Getting all the generated keys from a batch INSERT is not supported by this driver")
        for (item ← items)
          insertItem(txn, item)
      }
      case e: UnsupportedJDBCOperationException ⇒ {
        txn.rollbackSavepoint(savepoint)
        Logger.info("Getting all the generated keys from a batch INSERT is not supported by this driver")
        for (item ← items)
          insertItem(txn, item)
      }
    }

  }

  private def insertItem(txn: Transaction, item: Item) {
    item.id = txn.executeForKey(InsertItem, "item" -> item)
    Logger.info(item + " got ID: " + item.id.get)
  }

  private def drop(stm: Statement, drop: String) {
    try {
      stm.executeUpdate("drop " + drop)
    } catch {
      case _ ⇒ // Ignore 
    }
  }

  @Test
  def `test FreeMarker sequence expansion macro` {
    val broker = Broker(config)
    broker.readOnly() { session ⇒
      session.selectAll('selectItems, "ids" -> Seq(1, 2, 3))
    }
  }

  @Test
  def `test Velocity sequence expansion macro` {
    val broker = Broker(config)
    broker.readOnly() { session ⇒
      session.selectAll('selectItems_velocity, "ids" -> Seq(1, 2, 3))
    }
  }

  @After
  def teardown() {
    val conn = DriverManager.getConnection("jdbc:derby:" + dbName)
    val stm = conn.createStatement;
    drop(stm, "table CUSTOMER")
    drop(stm, "table CustOrder ")
    drop(stm, "table ITEM ")
    drop(stm, "table OrderItem ")
    drop(stm, "table Numbers ")
    drop(stm, "table FooBar ")
    drop(stm, "PROCEDURE KONGKAT")
    conn.commit()
    stm.close
    conn.close
    try
      DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true")
    catch {
      case e: SQLException => if (e.getSQLState != "08006") throw e
    }

  }

}

object TestBroker {
  val LogFormatter = new java.util.logging.Formatter {
    def format(lr: java.util.logging.LogRecord): String = {
      "%s %s: %s%n".format(
        new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new java.util.Date(lr.getMillis)),
        lr.getLevel,
        formatMessage(lr))
    }
  }

  val LogHandler = new java.util.logging.ConsoleHandler {
    setFormatter(LogFormatter)
  }

  val Logger = {
    val logger = java.util.logging.Logger.getLogger("org.orbroker")
    logger.setUseParentHandlers(false)
    logger.addHandler(LogHandler)
    logger
  }

  val sqlDir = new File("src/test/resources/example")
}
