package org.orbroker

import scala.compat.Platform.EOL
import org.junit._
import org.junit.Assert._
import org.orbroker._
import org.orbroker.pimp._

class TestSQLParser {
  val sql = Seq(
    " \t SELECT  /* Some comment here :bla */* FROM -- more comment here",
    "MyTable\t\tWhere  id = :obj.id  AND",
    "name = 'a = :name' OR age = :obj.age",
    "      ")
  def testParsed(parsed: ParserState, expected: String) {
    assertEquals(expected, parsed.sql.toString)
    assertEquals(2, parsed.params.size)
    assertEquals("obj.id", parsed.params(0))
    assertEquals("obj.age", parsed.params(1))
  }
  @Test
  def parseTrimmed {
    val expected = "SELECT * FROM MyTable Where id = ? AND name = 'a = :name' OR age = ? "
    val trimParsed = SQLParser.parse(sql, true)
    testParsed(trimParsed, expected)
  }
  @Test
  def parseUntrimmed {
    val expected = " \t SELECT  /* Some comment here :bla */* FROM -- more comment here" + EOL +
      "MyTable\t\tWhere  id = ?  AND" + EOL +
      "name = 'a = :name' OR age = ?" + EOL + "      "
    val notrimParsed = SQLParser.parse(sql, false)
    testParsed(notrimParsed, expected)
  }
  @Test
  def indexedParm {
    val sql = Seq("IN(:foo[0], :foo[1])")
    val parsed = SQLParser.parse(sql, true)
    assertEquals(2, parsed.params.size)
    assertEquals("foo[0]", parsed.params(0))
    assertEquals("foo[1]", parsed.params(1))
  }
}