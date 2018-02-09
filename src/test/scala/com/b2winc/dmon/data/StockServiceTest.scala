package com.b2winc.dmon.data

import java.sql.Timestamp

import com.b2winc.dmon.util.{BaseSparkContext, SharedSparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import org.apache.spark.sql.functions._

class StockServiceTest extends SharedSparkContext with MockFactory with Matchers {

  val stockRepository = new StockSparkRepository
  val toolboxService = new ToolboxService
  val itemsRepository = new ItemsSparkRepository
  val stockService = new StockService(stockRepository, itemsRepository, toolboxService)

  val expectedStock = Stock(sku = "1", warehouseCnpj = "1", ean = "1234", stockDate = DateTime.now.toString("yyyy-MM-dd"),
    availableStock = 30, brandGroup = "1")

  val dateNow = new Timestamp(DateTime.now.getMillis)

  val dfSchema = StructType(
    Array(
      StructField("sku", StringType),
      StructField("cnpj", StringType),
      StructField("stockDate", StringType),
      StructField("receivedDate", TimestampType),
      StructField("available", DoubleType),
      StructField("brandGroup", StringType),
      StructField("idCia", StringType)
    )
  )

  val dfItemSchema = StructType(
    Array(
      StructField("iteg_id", StringType),
      StructField("iteg_ean", StringType),
      StructField("iteg_fornec", StringType),
      StructField("iteg_id_cia", StringType)
    )
  )

  "the method stockTransformer" should {

    val sqc = sqlContext
    import sqc.implicits._

    "return a Stocks object when it receives a valid DataFrame" in {
      val rows = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          expectedStock.availableStock, expectedStock.brandGroup, "1")
      )

      val itemRows = Array(
        Row(expectedStock.sku, expectedStock.ean, "fornec", "1")
      )

      val mockedDataFrame = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)
      val mockedItemsDataFrame = sqlContext.createDataFrame(sc.parallelize(itemRows), dfItemSchema)

      val stocks = stockService.stockTransformer(mockedDataFrame, mockedItemsDataFrame, DateTime.now, DateTime.now)

      stocks shouldBe a[Stocks]
      stocks.stocks(0) shouldEqual expectedStock
    }

    "filter out stocks depending on the range date specified" in {

      val beforeYesterday = DateTime.now.minusDays(2)

      val rows = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, beforeYesterday.toString("yyyy-MM-dd"), new Timestamp(beforeYesterday.getMillis),
          10D, expectedStock.brandGroup, "1")
      )

      val itemRows = Array(
        Row(expectedStock.sku, expectedStock.ean, "fornec", "1")
      )
      val mockedItemsDataFrame = sqlContext.createDataFrame(sc.parallelize(itemRows), dfItemSchema)
      val mockedDataFrame = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val stocks = stockService.stockTransformer(mockedDataFrame, mockedItemsDataFrame, DateTime.now, DateTime.now)
      stocks shouldBe a[Stocks]
      stocks.stocks(0).availableStock shouldEqual (expectedStock.availableStock)

    }


  }



}
