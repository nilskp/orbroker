package org.orbroker.conv

import org.junit._
import org.junit.Assert._

import java.net._
import java.util._

class TestConverters {
  @Test
  def inet4() {
    val addr = InetAddress.getLocalHost.asInstanceOf[Inet4Address]
    val bytes = Inet4AddrBinaryConv.toJdbcType(addr)
    val backToAddr = InetAddress.getByAddress(bytes)
    assertEquals(addr, backToAddr)
  }
  @Test
  def uuid() {
    val uuid = UUID.randomUUID
    val bytes = UUIDBinaryConv.toJdbcType(uuid)
    val backToUUID = UUIDBinaryConv.fromBytes(bytes)
    assertEquals(uuid, backToUUID)
  }
}