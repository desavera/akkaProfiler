package com.b2winc.dmon.data

import java.sql.Timestamp

import com.b2winc.dmon.model.DataNotAvailableException
import grizzled.slf4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import com.b2winc.dmon.model.BrandConverters._
import com.b2winc.dmon.util.ConfigurationContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class Shipment(warehouseCnpj: String, warehouseUF: String, brand: String, nfIssuance: Timestamp, salesDate: Timestamp,
                    dispatchDate: Timestamp, nfId: Int, nfSerie: String, deliveryCity: String, deliveryUF: String, ean: String,
                    sku: String, IMEI: String)
case class Shipments(shipments: Seq[Shipment])

class ShipmentService(shipmentSparkRepository: ShipmentSparkRepository,
                      toolboxService: ToolboxService)
  extends Serializable {

  lazy val logger = Logger(getClass)

  def shipmentsTransformer(shipmentDF: DataFrame, initialDate : DateTime, finalDate: DateTime)(implicit sqlContext: HiveContext): Shipments = {
    import sqlContext.implicits._

    val mergedDF = shipmentDF
      .filter($"salesDate" >= ConfigurationContext.minimumDate)
      .filter($"salesDate" >= new Timestamp(initialDate.getMillis))
      .filter($"salesDate" <= new Timestamp(finalDate.getMillis))

    Shipments(shipments = mergedDF.map { case row =>
      Shipment(warehouseCnpj = row.getAs[String]("warehouseCnpj"),
        warehouseUF = row.getAs[String]("warehouseUf"),
        brand=row.getAs[String]("brand").asBrandWms,
        nfIssuance = row.getAs[Timestamp]("nfIssuance"),
        salesDate = row.getAs[Timestamp]("salesDate"),
        dispatchDate = row.getAs[Timestamp]("dispatchDate"),
        nfId = row.getAs[Int]("nfId"),
        nfSerie = row.getAs[String]("nfSerie"),
        deliveryCity = row.getAs[String]("deliveryCity"),
        deliveryUF = row.getAs[String]("deliveryUf"),
        ean = row.getAs[String]("ean"),
        sku = row.getAs[String]("sku"),
        IMEI = row.getAs[String]("imei"))
    }.collect)

  }

  def getShipments(initialDate: Option[String], finalDate: Option[String])(ec: ExecutionContext)(implicit sqlContext: HiveContext): Future[Shipments] = {

    val (initialDateDT, finalDateDT) = toolboxService.validateAndConvertDates(initialDate, finalDate)

    val getDFsTry = Try(shipmentSparkRepository.getCached()(sqlContext))

    getDFsTry match {
      case Success(shipmentsDF) => {
        Future(shipmentsTransformer(shipmentsDF.get,
          initialDateDT, finalDateDT)(sqlContext))(ec)
      }
      case Failure(e) => {
        logger.error("Failed to get shipments data.", e)
        throw new DataNotAvailableException(e.getMessage)
      }
    }
  }

}
