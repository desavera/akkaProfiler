package com.b2winc.dmon.web

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer

import scala.util.Try
import scala.concurrent.duration._
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.Http
import com.b2winc.dmon.auth.{AuthenticationService, Token, TokenRepository}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.model.StatusCodes._
import com.b2winc.dmon.data._
import com.b2winc.dmon.model.{Formatter, InsecureRequestException}
import com.b2winc.dmon.util.{ConfigurationContext, SparkContextHelper, SparkContextHelperIfc}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.{Source, StdIn}
import scala.util.{Failure, Success}
import Formatter._
import akka.http.scaladsl.server.directives.MethodDirectives
import com.b2winc.dmon.schedule.DataSnapshot
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import grizzled.slf4j.Logger

import scala.concurrent.duration._

object WebServer {

  lazy val logger = Logger[this.type]
  var contextHelper: SparkContextHelperIfc = SparkContextHelper
  val sc = contextHelper.createSparkContext
  lazy val sqlContext = contextHelper.createSqlContext

  var authenticationService = new AuthenticationService(new TokenRepository)
  var toolboxService = new ToolboxService
  var salesService = new SalesService(new SalesSparkRepository, toolboxService)
  var stockService = new StockService(new StockSparkRepository, new ItemsSparkRepository, toolboxService)
  var shipmentService = new ShipmentService(new ShipmentSparkRepository, toolboxService)

  implicit val system = ActorSystem("dmon-system")
  implicit val materializer = ActorMaterializer()

  // needed for the future map/flatmap in the end
  implicit val executionContext = system.dispatcher

  val rootEndpoint = "dmon"

  val authorizedUsers = Map(
    "samsung" -> ConfigurationContext.getStringConfigOrDefault("stagingPass", "d-m0n-Samsung2351")
  )

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: InsecureRequestException => {
        logger.debug("Insecure request.", ex)
        complete(HttpResponse(Unauthorized, entity=ex.message))
      }
      case ex: IllegalArgumentException => {
        logger.debug("Some method thrown an IllegalArgumentException.", ex)
        complete(HttpResponse(BadRequest, entity=ex.getMessage))
      }
      case ex: Throwable => {
        logger.warn("Unexpected error received.", ex)
        complete(HttpResponse(InternalServerError, entity="Unexepected error."))
      }
      case _ => complete(HttpResponse(InternalServerError, entity="Unexepected error"))
    }

  val route: Route =
    path(rootEndpoint / "token"){
      post{
        extractRequest { request =>
          val authorizationHeader = request.getHeader("Authorization")
          if(!authorizationHeader.isPresent) throw new InsecureRequestException("The Authorization header was not specified.")
          val httpHeader = authorizationHeader.get

          val authTokenFuture: Future[Option[Token]] = authenticationService
            .generateToken(httpHeader.value, authorizedUsers)(executionContext)

          onComplete(authTokenFuture){ maybeToken =>
            complete(maybeToken)
          }

        }
      }
    } ~
    path (rootEndpoint / "sale"){
      getOrReject("sale") {
          parameters('initialDate, 'finalDate){ (initialDate, finalDate) =>
          extractRequest { request =>
            validateToken(request)
            val salesFuture: Future[Sales] = salesService.getSales(Some(initialDate), Some(finalDate))(executionContext)(sqlContext)
            onComplete(salesFuture) { maybeSales =>
              maybeSales match {
                case Success(sales) => complete(sales)
                case Failure(e) => failWith(e)
              }
            }
          }
        }
      }
    } ~
    path (rootEndpoint / "stock") {
      getOrReject("stock") {
        parameters('initialDate, 'finalDate) { (initialDate, finalDate) =>
          extractRequest { request =>
            validateToken(request)
            val stockFuture: Future[Stocks] = stockService.getStock(Some(initialDate), Some(finalDate))(executionContext)(sqlContext)
            onComplete(stockFuture) { maybeStocks =>
              maybeStocks match {
                case Success(stocks) => complete(stocks)
                case Failure(e) => failWith(e)
              }
            }
          }
        }
      }
    } ~
    path (rootEndpoint / "shipments") {
      getOrReject("shipments") {
        parameters('initialDate, 'finalDate) { (initialDate, finalDate) =>
          extractRequest { request =>
            validateToken(request)
            val shipmentsFuture: Future[Shipments] = shipmentService.getShipments(Some(initialDate), Some(finalDate))(executionContext)(sqlContext)
            onComplete(shipmentsFuture) { maybeShipments =>
              maybeShipments match {
                case Success(shipments) => complete(shipments)
                case Failure(e) => failWith(e)
              }
            }
          }
        }
      }
    } ~
    path("health"){
      get {
        val dependency = new AtlasDependency(
          "DMON-API",
          "OK",
          "The dmon-api server", "")
        val response = new Health("OK",
          "The API is up and running",
          Seq[AtlasDependency](dependency))
        complete(response)
      }
    } ~
    path("resource-status"){
      get{
        val jsonStr = Source.fromInputStream(getClass.getResourceAsStream("/server-version.json"))
          .mkString
        val jsonObj = jsonStr.parseJson
        complete(jsonObj.convertTo[ResourceStatus])
      }
    }

  def getOrReject(endpoint: String): Directive0  = {
    if(ConfigurationContext.getBoolConfigOrDefault(s"$endpoint.active", true)) get
    else reject
  }

  def validateToken(request: HttpRequest) = {
    val tokenHeader = request.getHeader("X-B2W-TOKEN")
    if(!tokenHeader.isPresent) throw new InsecureRequestException("The authentication token was not informed.")
    val validToken = authenticationService.isValidToken(Some(tokenHeader.get.value))
    if(!validToken) throw new InsecureRequestException("The authentication token informed is not valid.")
  }

  def main(args: Array[String]): Unit = {

    //setup schedulers
    val scheduler = QuartzSchedulerExtension(system)
    val dataSnapshot = system.actorOf(Props[DataSnapshot])

    //schedule the initial cache heating
    system.scheduler.scheduleOnce(100 milliseconds, dataSnapshot, DataSnapshot.UpdateSales)
    system.scheduler.scheduleOnce(30 seconds, dataSnapshot, DataSnapshot.UpdateShipments)
    system.scheduler.scheduleOnce(90 seconds, dataSnapshot, DataSnapshot.UpdateStocks)

    //schedule the jobs that will update the cache daily
    scheduler.schedule("EveryDay1am", dataSnapshot, DataSnapshot.UpdateSales)
    scheduler.schedule("EveryDay1_5am", dataSnapshot, DataSnapshot.UpdateTmpStocksPosition)
    scheduler.schedule("EveryDay1_10am", dataSnapshot, DataSnapshot.UpdateStocks)
    scheduler.schedule("EveryDay1_20am", dataSnapshot, DataSnapshot.UpdateShipments)

    val port = ConfigurationContext.getIntConfigOrDefault("port", 8085)
    val address = ConfigurationContext.getStringConfigOrDefault("address", "0.0.0.0")

    val bindingFuture = Http().bindAndHandle(handler=route, address, port)

    println(s"Server online at http://$address:$port")
    //StdIn.readLine() // let it run until user presses return
    /*bindingFuture
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete(_ => system.terminate())*/
  }

}
