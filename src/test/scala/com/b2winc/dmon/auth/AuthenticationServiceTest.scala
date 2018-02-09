package com.b2winc.dmon.auth

import com.b2winc.dmon.model.InsecureRequestException
import com.b2winc.dmon.util.AuthenticationUtil
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec
import com.github.nscala_time.time.Imports._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._


class AuthenticationServiceTest extends FlatSpec with MockFactory {

  val authenticationService = new AuthenticationService(new TokenRepository)

  "credentialsMatcher" should "return a valid User" in {
    val userAndPassword = "foo:123"
    val user = authenticationService.credentialsMatcher(userAndPassword)
    assert(user.username.equals("foo"))
    assert(user.password.equals("123"))
  }

  "credentialsMatcher" should "gracefully fail when empty" in {
    assertThrows[InsecureRequestException] {
      authenticationService.credentialsMatcher("")
    }
  }

  "crendetialsMatcher" should "gracefully fail when invalid format" in {
    assertThrows[InsecureRequestException] {
      authenticationService.credentialsMatcher("foo123")
    }
  }

  //Zm9vOjEyMw== is foo:123 encoded in Base64 UTF-8
  "validateCredentials" should "return User" in {
    val storedCredentials = Map("foo" -> "123")
    val user = authenticationService.validateCredentials("Zm9vOjEyMw==", storedCredentials)
    assert(user.isDefined)
    assert(user.get.username.equals("foo"))
    assert(user.get.password.equals("123"))
  }

  "validateCredentials" should "return empty when the password is incorrect" in {
    val storedCredentials = Map("foo" -> "000")
    val user = authenticationService.validateCredentials("Zm9vOjEyMw==", storedCredentials)
    assert(!user.isDefined)
  }

  "validateCredentials" should "return false when the user does not exists" in {
    val storedCredentials = Map("bar" -> "123")
    val user = authenticationService.validateCredentials("Zm9vOjEyMw==", storedCredentials)
    assert(!user.isDefined)
  }

  "validateCredentials" should "return false when the storedCredentials are empty" in {
    val user = authenticationService.validateCredentials("Zm9vOjEyMw==", Map())
    assert(!user.isDefined)
  }

  "validateCredentials" should "gracefully fail when stored credentials are null" in {
    assertThrows[InsecureRequestException] {
      authenticationService.validateCredentials("Zm9vOjEyMw==", null)
    }
  }

  implicit val ec = ExecutionContext.global

  //the actual tests
  "generateToken" should "return a valid token" in {
    authenticationService.systemTime = AuthenticationUtil.SystemTimeTest
    val token = Await.result(
      authenticationService.generateToken("Zm9vOjEyMw==", Map("foo" -> "123"))(ec),
      500 millisecond)
    assert(token.isDefined)
    assert(token.get.token.equals("A96988FFF2B25D503FA22022C24AE8AE"))
    val hourIn30Min = DateTime.now.plusMinutes(30).toString("yyyy-MM-dd HH:mm")
    assert(token.get.expiresAt.toString("yyyy-MM-dd HH:mm").equals(hourIn30Min))
  }

  "generateToken with null credentials" should "gracefully fail" in {
    assertThrows[InsecureRequestException] {
      Await.result(authenticationService.generateToken(null, Map("foo" -> "123"))(ec), 500 millisecond)
    }
  }

  "generateToken with invalid credentials" should "be empty" in {
    assertThrows[InsecureRequestException] {
      val token = Await.result(authenticationService.generateToken("Zm9vOjEyMw==", Map("bar" -> "123"))(ec), 500 millisecond)
    }
  }

}
