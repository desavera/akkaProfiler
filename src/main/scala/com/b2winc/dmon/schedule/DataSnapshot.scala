package com.b2winc.dmon.schedule

import akka.actor.Actor
import akka.actor.Actor.Receive
import com.b2winc.dmon.data._
import com.b2winc.dmon.schedule.DataSnapshot.{UpdateSales, UpdateShipments, UpdateStocks, UpdateTmpStocksPosition}
import com.b2winc.dmon.util.SparkContextHelper
import grizzled.slf4j.Logger

object DataSnapshot {
  case object UpdateSales
  case object UpdateStocks
  case object UpdateTmpStocksPosition
  case object UpdateShipments
  case object Xi
}

class DataSnapshot extends Actor {

  lazy val logger = Logger(getClass)

  val salesSparkRepository = new SalesSparkRepository
  val stockSparkRepository = new StockSparkRepository
  val shipmentSparkRepository = new ShipmentSparkRepository

  override def receive: Receive = {
    case UpdateSales => {
      logger.info("Will update the sales snapshot.")
      salesSparkRepository.updateCached()(SparkContextHelper.createSqlContext)
      //collect in order to run the lazy operations
      SalesSparkRepository.salesDF.get.collect()
    }
    case UpdateStocks => {
      logger.info("Will update the stock snapshot.")
      val sqlContext = SparkContextHelper.createSqlContext
      stockSparkRepository.updateCached()(sqlContext)
      StockSparkRepository.stockDF.get.collect()
    }
    case UpdateTmpStocksPosition => {
      logger.info("Will update the stock snapshot.")
      val sqlContext = SparkContextHelper.createSqlContext
      stockSparkRepository.updateTmpTable()(sqlContext)
    }
    case UpdateShipments => {
      logger.info("Will update the shipments snapshot.")
      shipmentSparkRepository.updateCached()(SparkContextHelper.createSqlContext)
      ShipmentSparkRepository.shipmentDF.get.collect()
    }
  }

}
