package com.b2winc.dmon.data

import java.sql.Timestamp

import com.b2winc.dmon.util.{BaseSparkContext, SharedSparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.{AsyncFlatSpec, Matchers, WordSpec}

import scala.concurrent.ExecutionContext

class ShipmentServiceTest extends SharedSparkContext
  with MockFactory with Matchers with BaseSparkContext {

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

  "The method shipmentTransformer" should {

    "filter out sales by salesDate out of the specified date range" in {
      val shipmentRows = Array(
        Row("cnpj", "1", "uf", "city", "uf2", "ean", "sku", "imei",
          new Timestamp(today.getMillis), 10, "2", new Timestamp(today.getMillis),
          new Timestamp(today.getMillis)),
        Row("cnpj", "1", "uf", "city", "uf2", "ean", "sku", "imei",
          new Timestamp(today.getMillis), 20, "1", new Timestamp(today.minusDays(5).getMillis),
          new Timestamp(today.getMillis))
      )

      val shipmentsDfMock = sqlContext.createDataFrame(sc.parallelize(shipmentRows), shipmentsDfSchema)

      val initialDate = DateTime.now.minusDays(2)
      val finalDate = DateTime.now

      val result = shipmentService.shipmentsTransformer(shipmentsDfMock, initialDate, finalDate)

      result.shipments.size shouldEqual 1
      result.shipments(0).nfId shouldEqual 10
      result.shipments(0).nfSerie shouldEqual "2"
    }

    "return a valid Shipments" in {

      val shipmentRows = Array(
        Row("cnpj", "1", "uf", "city", "uf2", "ean", "sku", "imei",
          new Timestamp(today.getMillis), 10, "2", new Timestamp(today.getMillis),
          new Timestamp(today.getMillis))
      )

      val shipmentsDfMock = sqlContext.createDataFrame(sc.parallelize(shipmentRows), shipmentsDfSchema)

      val initialDate = DateTime.now.minusDays(2)
      val finalDate = DateTime.now

      val result = shipmentService.shipmentsTransformer(shipmentsDfMock, initialDate, finalDate)

      val shipment = result.shipments(0)

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
      shipment.nfSerie shouldEqual "2"
    }

    "return an empty Seq of Shipment in Shipments when when shipmentDF is empty" in {

      val shipmentDF = sqlContext.createDataFrame(sc.emptyRDD[Row], shipmentsDfSchema)

      val initialDate = DateTime.now.minusDays(2)
      val finalDate = DateTime.now

      val result = shipmentService.shipmentsTransformer(shipmentDF, initialDate, finalDate)

      result.shipments.size shouldEqual 0
    }

  }
}
