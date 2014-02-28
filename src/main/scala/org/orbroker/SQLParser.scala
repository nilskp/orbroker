package org.orbroker

import scala.compat.Platform.EOL
import scala.collection.immutable.SortedMap
import org.orbroker._
import org.orbroker.exception._
import java.io._

/**
 * Messy SQL parser, but it does the job.
 * @author Nils Kilden-Pedersen
 */
private[orbroker] object SQLParser {

  def parse(sql: Seq[String], doTrim: Boolean): ParserState = {
    val parserState = new ParserState(doTrim)
    sql foreach { handleLine(_, parserState) }
    if (parserState.state == State.InSpace) parserState.state = State.Normal
    if (parserState.state != State.Normal) throw new ConfigurationException("SQL parsing error. Ended in state " + parserState.state, null)
    parserState
  }

  private def handleLine(line: String, state: ParserState) {
    if (state.sql.length > 0) {
      if (!state.doTrim) {
        state.sql append EOL
      } else if (state.state == State.Normal && state.sql.last != ' ') {
        state.state = State.InSpace
        state.sql append ' '
      }
    }
    var pos = 0
    while (pos < line.length) {
      state.state match {
        case State.Normal ⇒ pos = normal(line, pos, state)
        case State.InCComment ⇒ pos = cComment(line, pos, state)
        case State.InEOLComment ⇒ pos = eolComment(line, pos, state)
        case State.InString ⇒ pos = inString(line, pos, state)
        case State.InParm ⇒ pos = inParm(line, pos, state)
        case State.InSpace ⇒ pos = inSpace(line, pos, state)
        case State.InParmIdx ⇒ pos = inParmIdx(line, pos, state)
      }
    }
    state.state match {
      case State.InEOLComment ⇒ state.state = State.Normal
      case State.InParm ⇒ state.state = State.Normal
      case _ ⇒
    }
  }

  private def inSpace(line: String, pos: Int, state: ParserState): Int = {
    val c = charAt(line, pos)
    if (c == ' ' || c == '\t') {
      if (!state.doTrim) state.sql append c
      pos + 1
    } else {
      state.state = State.Normal
      pos
    }
  }

  private def eolComment(line: String, pos: Int, state: ParserState): Int = {
    if (!state.doTrim) state.sql append line.substring(pos, line.length)
    line.length
  }
  private def inString(line: String, pos: Int, state: ParserState): Int = {
    state.sql append charAt(line, pos)
    if (charAt(line, pos) == '\'') state.state = State.Normal
    pos + 1
  }
  private def inParm(line: String, pos: Int, state: ParserState): Int = charAt(line, pos) match {
    case c@('[') ⇒ state.state = State.InParmIdx; state.currentParm append c; pos + 1
    case c if !isParmChar(c) ⇒ state.state = State.Normal; pos
    case c ⇒ state.currentParm append c; pos + 1
  }

  private def inParmIdx(line: String, pos: Int, state: ParserState): Int = charAt(line, pos) match {
    case c@(']') ⇒ state.currentParm append c; state.state = State.Normal; pos + 1
    case c if (isInteger(c)) ⇒ state.currentParm append c; pos + 1
    case c ⇒ throw new IllegalArgumentException("Encountered '%c' at index position: %s".format(c, state.currentParm))
  }

  private def normal(line: String, pos: Int, state: ParserState): Int = charAt(line, pos) match {
    case ':' if isParmFirstChar(charAt(line, pos + 1)) ⇒ {
      state.currentParm append line(pos + 1)
      state.sql append '?'
      state.state = State.InParm
      pos + 2
    }
    case c@(' ' | '\t') ⇒ {
      state.state = State.InSpace
      if (state.doTrim) {
        if (state.sql.length > 0) state.sql append ' '
      } else {
        state.sql append c
      }
      pos + 1
    }
    case '\'' ⇒ state.state = State.InString; state.sql append '\''; pos + 1
    case '/' if charAt(line, pos + 1) == '*' ⇒ {
      if (!state.doTrim) state.sql append "/*"
      state.state = State.InCComment;
      pos + 2
    }
    case '-' if charAt(line, pos + 1) == '-' ⇒ {
      if (!state.doTrim) state.sql append "--"
      state.state = State.InEOLComment
      pos + 2
    }
    case c ⇒ state.sql append c; pos + 1
  }
  private def cComment(line: String, pos: Int, state: ParserState): Int = {
    if (charAt(line, pos) == '*' && charAt(line, pos + 1) == '/') {
      if (!state.doTrim) state.sql append "*/"
      state.state = State.Normal
      pos + 2
    } else {
      if (!state.doTrim) state.sql append charAt(line, pos)
      pos + 1
    }
  }

  private def charAt(line: String, pos: Int) = if (pos < line.length) line.charAt(pos) else '\n'
  private def isParmChar(c: Char) = isInteger(c) || c == '.' || isParmFirstChar(c) || c == '$'
  private def isParmFirstChar(c: Char) = {
    c >= 'a' && c <= 'z' ||
      c >= 'A' && c <= 'Z' ||
      c == '_'
  }
  private def isInteger(c: Char) = c >= '0' && c <= '9'

}
private[orbroker] class ParserState(val doTrim: Boolean) {
  import scala.collection.mutable.ArrayBuffer
  private var _state: State = State.Normal
  def state = _state
  def state_=(newState: State) = {
    if (newState == State.InParmIdx) {
      assert(_state == State.InParm)
    } else if (_state == State.InParm || _state == State.InParmIdx) {
      parms += currentParm.toString
      currentParm.setLength(0)
    }
    _state = newState
  }
  val sql = new StringBuilder(128)
  val currentParm = new StringBuilder
  private val parms = new ArrayBuffer[String]
  def params = new scala.collection.immutable.IndexedSeq.Impl(parms)
}

private sealed trait State
private object State {
  case object InEOLComment extends State
  case object InCComment extends State
  case object InString extends State
  case object InParm extends State
  case object InParmIdx extends State
  case object Normal extends State
  case object InSpace extends State
}