package com.b2winc.dmon.auth

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

import com.b2winc.dmon.model.InsecureRequestException
import com.b2winc.dmon.util.ParameterHelper
import com.github.nscala_time.time.Imports._
//import grizzled.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}


case class User(username: String, password: String)

case class Token(token: String, expiresAt: DateTime)

// This design decision was made to make AuthenticationService class testable. Current version of ScalaMock don't support mocking singletons
object SystemTime extends SystemTimeIfc {
  override def currentTimeInMillis: Long = {
    System.currentTimeMillis
  }
}
trait SystemTimeIfc {
  def currentTimeInMillis: Long = ???
}

class AuthenticationService(tokenRepository: TokenRepository) {

  //val logger = Logger(classOf[AuthenticationService])

  var systemTime: SystemTimeIfc = SystemTime
  /**
    * This method will generate a token if the specified credentials are valid
    * @param basicAuthentication
    * @return
    */
  def generateToken(basicAuthentication: String,
                    validCredentialsMap: Map[String, String])(ec: ExecutionContext): Future[Option[Token]] = {

    ifThrownThen(() => ParameterHelper.require(basicAuthentication), new InsecureRequestException("The specified credentials are invalid."))
    val defaultExpireMinutes = 30
    val user = validateCredentials(basicAuthentication.replace("Basic ", ""), validCredentialsMap)
    user match {
      case Some(user) => Future{
        val token = Token(token = generateHashMD5(user), expiresAt = DateTime.now.plusMinutes(defaultExpireMinutes))
        TokenRepository.tokenMap.put(token.token, token)
        Some(token)
      }(ec)
      case None => throw new InsecureRequestException("The specified user is not authorized.")
      case _ => Future(Option.empty)(ec)
    }
  }

  def isValidToken(token: Option[String]): Boolean = {
    if(token.isDefined){
      //get the persisted tokens
      val persistedTokens = tokenRepository.getPersistedTokens
      val persistedToken = persistedTokens.get(token.get)
      require(persistedToken.isDefined, "The informed token is invalid.")
      ifThrownThen(
        ()=>require(!hasExpired(persistedToken.get.expiresAt)),
        new InsecureRequestException("The informed token has expired. Please request a new token.")
      )
      return true
    }
    return false
  }

  def hasExpired(date: DateTime): Boolean = {
    date.compareTo(DateTime.now) < 0
  }

  def generateHashMD5(user: User): String = {

    val timeInString = systemTime.currentTimeInMillis.toString
    val usernameAndPass = user.username.concat(user.password)
    val md5Bytes = MessageDigest.getInstance("MD5").digest(usernameAndPass.concat(timeInString).getBytes)
    val hexBinaryAdapter = new HexBinaryAdapter
    hexBinaryAdapter.marshal(md5Bytes)

  }


  /**
    * This method checks against a datasource if the specified credentials are valid
    * @param encodedCredentials - The base64 encoded credentials in the following format user:password
    * @return
    */
  def validateCredentials(encodedCredentials: String, validCredentialsMap: Map[String, String]): Option[User] = {
    val userAndPassword = new String(Base64.getDecoder.decode(encodedCredentials),
                                    StandardCharsets.UTF_8)

    ifThrownThen(() => ParameterHelper.require(validCredentialsMap), new InsecureRequestException("The valid credentials map is empty."))
    val user = credentialsMatcher(userAndPassword)
    validCredentialsMap.get(user.username) match {
      case Some(user.password) => Some(user)
      case _ => Option.empty
    }
  }

  def credentialsMatcher(credentials: String) = credentials.split(":") match {
    case Array(user, pass) => User(user, pass)
    case _ => throw new InsecureRequestException("The credentials are not in the valid format user:password.")
  }

  def ifThrownThen(required: () => Any, toThrow: Throwable): Unit = {
    try{
      required()
    } catch {
      case e:Exception => throw toThrow
    }
  }
}
