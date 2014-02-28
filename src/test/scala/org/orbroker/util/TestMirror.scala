package org.orbroker.util

import org.junit._
import org.junit.Assert._

class TestMirror {

  var mirror: Mirror = null

  @Before
  def setup() {
    mirror = new Mirror
  }

  @Test
  def simple() {
    object Foo {
      val bar = 42
    }
    assertEquals(42, mirror.leafValue(Foo, "bar"))
    assertEquals(42, mirror.leafValue(Foo, "bar"))
  }

  @Test
  def nullInTheMiddle() {
    object Foo {
      val bar = new {
        val doo = null
      }
    }
    assertNull(mirror.leafValue(Foo, "bar.doo.dar"))
    assertNull(mirror.leafValue(Foo, "bar.doo.dar"))
  }

  @Test
  def noneInTheMiddle() {
    object Foo {
      val bar = new {
        val doo = None
      }
    }
    assertNull(mirror.leafValue(Foo, "bar.doo.dar"))
    assertNull(mirror.leafValue(Foo, "bar.doo.dar"))
  }

  @Test
  def arrayIndexed() {
    object Foo {
      val array = {
        val ar = new Array[Float](3)
        ar(0) = 1.1f
        ar(1) = 2.2f
        ar(2) = 3.3f
        ar
      }
    }
    assertEquals(1.1f, mirror.leafValue(Foo, "array[0]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "array.1"))
    assertEquals(3.3f, mirror.leafValue(Foo, "array[2]"))
    assertEquals(1.1f, mirror.leafValue(Foo, "array[0]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "array.1"))
    assertEquals(3.3f, mirror.leafValue(Foo, "array[2]"))
  }

  @Test
  def javaList() {
    object Foo {
      val list = {
        val list = new java.util.LinkedList[Float]
        list add 1.1f
        list add 2.2f
        list add 3.3f
        list
      }
    }
    assertEquals(1.1f, mirror.leafValue(Foo, "list[0]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "list.1"))
    assertEquals(3.3f, mirror.leafValue(Foo, "list[2]"))
    assertEquals(1.1f, mirror.leafValue(Foo, "list[0]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "list.1"))
    assertEquals(3.3f, mirror.leafValue(Foo, "list[2]"))
  }

  @Test
  def scalaSeq() {
    object Foo {
      val seq = Seq(1.1f, 2.2f, 3.3f)
    }
    assertEquals(1.1f, mirror.leafValue(Foo, "seq[0]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "seq.1"))
    assertEquals(3.3f, mirror.leafValue(Foo, "seq[2]"))
    assertEquals(1.1f, mirror.leafValue(Foo, "seq[0]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "seq.1"))
    assertEquals(3.3f, mirror.leafValue(Foo, "seq[2]"))
  }

  @Test
  def scalaMap() {
    object Foo {
      val bar = new {
        val map = new scala.collection.mutable.HashMap[String, Float]
        map += ("0" -> 0.0f)
        map += ("1" -> 1.1f)
        map += ("abc" -> 2.2f)
        map += ("def" -> 3.3f)
      }
    }
    assertEquals(0.0f, mirror.leafValue(Foo, "bar.map.0"))
    assertEquals(1.1f, mirror.leafValue(Foo, "bar.map[1]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "bar.map.abc"))
    assertEquals(3.3f, mirror.leafValue(Foo, "bar.map[def]"))
    assertEquals(0.0f, mirror.leafValue(Foo, "bar.map.0"))
    assertEquals(1.1f, mirror.leafValue(Foo, "bar.map[1]"))
    assertEquals(2.2f, mirror.leafValue(Foo, "bar.map.abc"))
    assertEquals(3.3f, mirror.leafValue(Foo, "bar.map[def]"))
  }
  @Test
  def javaMap() {
    object Foo {
      val bar = new {
        val map = new java.util.HashMap[String, Float]
        map.put("0", 0.0f)
        map.put("1", 1.1f)
        map.put("abc", 2.2f)
        map.put("def", 3.3f)
      }
    }
    assertEquals(0.0f, mirror.leafValue(Foo, "bar.map[0]"))
    assertEquals(1.1f, mirror.leafValue(Foo, "bar.map.1"))
    assertEquals(2.2f, mirror.leafValue(Foo, "bar.map[abc]"))
    assertEquals(3.3f, mirror.leafValue(Foo, "bar.map.def"))
    assertEquals(0.0f, mirror.leafValue(Foo, "bar.map[0]"))
    assertEquals(1.1f, mirror.leafValue(Foo, "bar.map.1"))
    assertEquals(2.2f, mirror.leafValue(Foo, "bar.map[abc]"))
    assertEquals(3.3f, mirror.leafValue(Foo, "bar.map.def"))
  }

  @Test
  def javaBean() {
    object Foo {
      def getBar = Some(42)
    }
    assertEquals(42, mirror.leafValue(Foo, "bar"))
    assertEquals(42, mirror.leafValue(Some(Foo), "bar"))
    
    class Beano(val myProp: Int)
    assertEquals(42, mirror.leafValue(new Beano(42), "myProp"))
  }
}