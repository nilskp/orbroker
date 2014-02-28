package test.orbroker.example.sp

object SPCall {
  def kongKat(f: String, s: Array[String], result: Array[String]) {
    result(0) = f + "-"  + s(0)
    s(0) = s(0).toUpperCase
  }
}
