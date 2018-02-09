package com.b2winc.dmon.web

import java.sql.Timestamp

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpec}
import akka.http.scaladsl.server._
import Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpHeader, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import com.b2winc.dmon.auth.{AuthenticationService, Token, TokenRepository}
import com.b2winc.dmon.data._
import com.b2winc.dmon.model.Formatter
import com.b2winc.dmon.util.{AuthenticationUtil, BaseSparkContext, SparkContextHelperIfc, SharedSparkContext}
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import Formatter._
import org.apache.spark.sql.hive.HiveContext

import scala.concurrent.{ExecutionContext, Future}

class WebServerRoutesTest extends SharedSparkContext with Matchers with ScalatestRouteTest with Inside with MockFactory
  with BeforeAndAfter {

  //the exception handler to test
  implicit def myExceptionHandler: ExceptionHandler = WebServer.myExceptionHandler

  WebServer.contextHelper = SparkContextHelperTest

  //the route to test
  val route = WebServer.route

  "The dmon endpoint" should {

    "return a valid token for POST requests with valid Authorization header in token path" in {

      val authenticationService = new AuthenticationService(new TokenRepository)
      authenticationService.systemTime = AuthenticationUtil.SystemTimeTest
      //inject the mocked authenticationService
      WebServer.authenticationService = authenticationService

      val authorizationHeader = RawHeader("Authorization", "Basic c2Ftc3VuZzpkLW0wbi1TYW1zdW5nMjM1MQ==")

      Post("/dmon/token").withHeaders(authorizationHeader) ~> route ~> check {
        implicit val unmarshaller = SprayJsonSupport.sprayJsonUnmarshallerConverter(tokenFormat)
        val response = responseAs[Token]
        response shouldBe a [Token]
        response.token should equal ("597896A69B9A87B20CCB7905DF795298")
        response.expiresAt shouldBe a [DateTime]
        response.expiresAt.toString("yyyy-MM-dd HH:mm") should equal (DateTime.now.plusMinutes(30).toString("yyyy-MM-dd HH:mm"))
      }
    }

    "return Unauthorized for POST requests without Authorization header" in {
      Post("/dmon/token") ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[String] shouldEqual "Invalid request, a security requirement was not satisfied. The Authorization header was not specified."
      }
    }

    "return Unauthorized for POST requests with an empty Authorization header" in {
      val emptyAuthorizationHeader = RawHeader("Authorization", "")
      Post("/dmon/token").withHeaders(emptyAuthorizationHeader) ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[String] shouldEqual "Invalid request, a security requirement was not satisfied. The specified credentials are invalid."
      }
    }

    "return Unauthorized for POST requests with an invalid format Authorization header" in {
      val invalidAuthorizationHeader = RawHeader("Authorization", "Basic test")
      Post("/dmon/token").withHeaders(invalidAuthorizationHeader) ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[String] shouldEqual "Invalid request, a security requirement was not satisfied. The credentials are not in the valid format user:password."
      }
    }

    "return Unauthorized for POST requests with an unauthorized user in Authorization header" in {
      val unauthorizedUser = RawHeader("Authorization", "Basic dGVzdDp0ZXN0")
      Post("/dmon/token").withHeaders(unauthorizedUser) ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[String] shouldEqual "Invalid request, a security requirement was not satisfied. The specified user is not authorized."
      }
    }

    "return Unauthorized for POST request with an invalid password in Authorization header" in {
      val invalidPassUser = RawHeader("Authorization", "Basic c2Ftc3VuZzplcnJv")
      Post("/dmon/token").withHeaders(invalidPassUser) ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[String] shouldEqual "Invalid request, a security requirement was not satisfied. The specified user is not authorized."
      }
    }
  }

  "The sale endpoint" should {

    before {
      //make sure that the service instace is reseted for each tested
      WebServer.salesService = new SalesService(new SalesSparkRepository, new ToolboxService)
    }

    val tokenHeader = RawHeader("X-B2W-TOKEN", "12345")
    TokenRepository.tokenMap.put("12345", Token("12345", DateTime.now.plusMinutes(15)))
    val yesterday = DateTime.now.minusDays(1).toString("yyyy-MM-dd")
    val today = DateTime.now.toString("yyyy-MM-dd")
    val todayInvalidFormat = DateTime.now.toString("dd-MM-yyyy")
    val twoMonthsAgo = DateTime.now.minusMonths(2).toString("yyyy-MM-dd")

    "return Unauthorized for GET requests without a token" in {
      Get(s"/dmon/sale?initialDate=$yesterday&finalDate=$today") ~> route ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[String] shouldEqual "Invalid request, a security requirement was not satisfied. The authentication token was not informed."
      }
    }

    "invoke getSales with correct parameters for GET requests" in {
      val mockSalesService = mock[SalesService]
      WebServer.salesService = mockSalesService

      val todayWithZeros = DateTime.now
        .withHourOfDay(0)
        .withMinuteOfHour(0)
        .withSecondOfMinute(0)
        .withMillisOfSecond(0)

      val sale = new Sale(warehouseCnpj = "123", brand = "", ean = "1", quantity = 1, sku = "",
        salesDate = new Timestamp(todayWithZeros.getMillis), ufTarget = Some(""))

      (mockSalesService.getSales(_: Option[String], _: Option[String])(_:ExecutionContext)(_:HiveContext))
        .expects(Some(yesterday), Some(today), *, *)
        .returning(Future(Sales(sales=Array(sale))))
        .once

      Get(s"/dmon/sale?initialDate=$yesterday&finalDate=$today").withHeaders(tokenHeader) ~> route ~> check {
        implicit val unmarshaller = SprayJsonSupport.sprayJsonUnmarshallerConverter(salesFormat)
        responseAs[Sales] shouldEqual Sales(sales=Array(sale))
      }
    }

    "fail gracefully when the initialDate is in invalid format" in {
      Get(s"/dmon/sale?initialDate=$todayInvalidFormat&finalDate=$today").withHeaders(tokenHeader) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual s"The date $todayInvalidFormat 00:00 was not specified in a valid format (YYYY-MM-DD)."
      }
    }

    "fail gracefully when the finalDate is in invalid format" in {
      Get(s"/dmon/sale?initialDate=$today&finalDate=$todayInvalidFormat").withHeaders(tokenHeader) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual s"The date $todayInvalidFormat 23:59 was not specified in a valid format (YYYY-MM-DD)."
      }
    }

    "fail gracefully when the initialDate is older then one month" in {
      Get(s"/dmon/sale?initialDate=$twoMonthsAgo&finalDate=$todayInvalidFormat").withHeaders(tokenHeader) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual "The specified initialDate is older than 1 month."
      }
    }

  }

  "The resource-status endpoint" should  {

    "return a valid resource-status json" in {
      Get("/resource-status") ~> route ~> check {
        implicit val unmarshaller = SprayJsonSupport.sprayJsonUnmarshallerConverter(resourceStatusFormat)
        status shouldEqual StatusCodes.OK
        val expectedRS = ResourceStatus("REPLACE_WITH_NAME", "REPLACE_WITH_BUILD", "REPLACE_WITH_VERSION")
        responseAs[ResourceStatus] shouldEqual expectedRS
      }
    }

  }

  "The health endpoint" should {

    "return a valid health" in {
      Get("/health") ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }

}
