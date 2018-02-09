package com.b2winc.dmon.auth

import scala.collection.mutable.Map

object TokenRepository {
  val tokenMap: Map[String, Token] = Map()
}

class TokenRepository {

  def getPersistedTokens(): Map[String, Token] = {
    return TokenRepository.tokenMap
  }

}
