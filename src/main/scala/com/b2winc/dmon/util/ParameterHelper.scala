package com.b2winc.dmon.util


object ParameterHelper {

  def require(any: Any, messageWhenNotFound: String = "") = any match {
    case null => throw new IllegalArgumentException(messageWhenNotFound)
    case "" => throw new IllegalArgumentException(messageWhenNotFound)
    case _ => "ok"
  }

}
