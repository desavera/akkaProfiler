package com.b2winc.dmon.data

import java.sql.Timestamp

import com.b2winc.dmon.util.AsyncSharedSparkContext
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.joda.time.DateTime
import org.scalamock.scalatest.{AsyncMockFactory, MockFactory}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.ExecutionContext

class ShipmentServiceAsyncTest extends AsyncSharedSparkContext
  with AsyncMockFactory with Matchers {

  lazy implicit val sqc = sqlContext
  implicit val ec = ExecutionContext.global

  val mShipmentSparkRepo = mock[ShipmentSparkRepository]
  val mToolboxService = mock[ToolboxService]

  val shipmentService = new ShipmentService(mShipmentSparkRepo, mToolboxService)

  val shipmentsDfSchema = StructType(
    Array(
      StructField("warehouseCnpj", StringType),
      StructField("brand", StringType),
      StructField("warehouseUf", StringType),
      StructField("deliveryCity", StringType),
      StructField("deliveryUf", StringType),
      StructField("ean", StringType),
      StructField("sku", StringType),
      StructField("imei", StringType),
      StructField("dispatchDate", TimestampType),
      StructField("nfId", IntegerType),
      StructField("nfSerie", StringType),
      StructField("salesDate", TimestampType),
      StructField("nfIssuance", TimestampType)
    )
  )

  val today = DateTime
    .now
    .withMillisOfSecond(0)
    .withSecondOfMinute(0)
    .withMinuteOfHour(0)


  "The method getShipments" should {

    "return a future with Shipments" in {

      val initialDate = DateTime.now.minusDays(15)
      val finalDate = DateTime.now

      (mToolboxService.validateAndConvertDates _)
        .expects(Some(initialDate.toString("yyyy-MM-dd")), Some(finalDate.toString("yyyy-MM-dd")))
        .returning((initialDate, finalDate))

      val shipmentRows = Array(
        Row("cnpj", "1", "uf", "city", "uf2", "ean", "sku", "imei",
          new Timestamp(today.getMillis), 10, "1", new Timestamp(today.minusDays(1).getMillis),
          new Timestamp(today.getMillis))
      )

      val shipmentsDfMock = sqlContext.createDataFrame(sc.parallelize(shipmentRows), shipmentsDfSchema)

      (mShipmentSparkRepo.getCached()(_: HiveContext))
        .expects(sqc)
        .returning(Some(shipmentsDfMock))
        .once

      val shipmentsFuture = shipmentService.getShipments(
        Some(initialDate.toString("yyyy-MM-dd")),
        Some(finalDate.toString("yyyy-MM-dd")))(ec)

      shipmentsFuture map { shipments =>
        shipments.shipments.size shouldEqual 1
        val shipment = shipments.shipments(0)
        shipment.warehouseCnpj shouldEqual "cnpj"
        shipment.brand shouldEqual "SHOP"
        shipment.deliveryCity shouldEqual "city"
        shipment.deliveryUF shouldEqual "uf2"
        shipment.warehouseUF shouldEqual "uf"
        shipment.ean shouldEqual "ean"
        shipment.sku shouldEqual "sku"
        shipment.IMEI shouldEqual "imei"
        shipment.dispatchDate shouldEqual new Timestamp(today.getMillis)
        shipment.nfId shouldEqual 10
        shipment.nfSerie shouldEqual "1"
      }
    }
  }
}
