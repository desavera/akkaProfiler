package com.b2winc.dmon.data

import java.sql.Timestamp

import com.b2winc.dmon.util.{SharedSparkContext}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers}
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import org.joda.time.DateTime


class SalesServiceTest extends SharedSparkContext with MockFactory with Matchers {

  lazy val salesRepository = new SalesSparkRepository
  lazy val toolboxService = new ToolboxService
  lazy val salesService = new SalesService(salesRepository, toolboxService)

  "The salesTransformer" should {

    //setup for tests
    val expectedSale = new Sale(warehouseCnpj = "12345", brand = "SUBA", salesDate = new Timestamp(DateTime.now.getMillis),
      ean = "12345", sku = "123sku", ufTarget = Some("SP"), quantity = 3D)

    val dfSchema = StructType(
      Array(
        StructField("cnpj", StringType),
        StructField("brand", StringType),
        StructField("salesDate", TimestampType),
        StructField("ean", StringType),
        StructField("sku", StringType),
        StructField("ufTarget", StringType),
        StructField("quantity", DoubleType)
      )
    )

    "return an Seq of Sales when it receives a valid DataFrame" in {

      val rows = Array(
        Row(expectedSale.warehouseCnpj, "3", expectedSale.salesDate,
          expectedSale.ean, expectedSale.sku, expectedSale.ufTarget.get, expectedSale.quantity)
      )

      val salesDF = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val salesSeq = salesService.salesTransformer(salesDF, DateTime.now.minusDays(1), DateTime.now)

      salesSeq shouldBe a[Sales]
      salesSeq.sales.size shouldEqual (1)
      salesSeq.sales(0) shouldEqual (expectedSale)
    }

    "aggregate rows and sum quantity when the DataFrame has several rows" in {

      val rows = Array(
        Row(expectedSale.warehouseCnpj, "3", expectedSale.salesDate,
          expectedSale.ean, expectedSale.sku, expectedSale.ufTarget.get, expectedSale.quantity),
        Row(expectedSale.warehouseCnpj, "3", expectedSale.salesDate,
          expectedSale.ean, expectedSale.sku, expectedSale.ufTarget.get, 5D)
      )

      val salesDF = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val salesSeq = salesService.salesTransformer(salesDF, DateTime.now.minusDays(1), DateTime.now)

      salesSeq shouldBe a [Sales]
      salesSeq.sales.size shouldEqual (1)
      salesSeq.sales(0).quantity shouldEqual (8)

    }

    "filter out sales depending on the range date specified" in {

      val beforeYesterday = new Timestamp(DateTime.now.minusDays(2).getMillis)

      val rows = Array(
        Row(expectedSale.warehouseCnpj, "3", expectedSale.salesDate,
          expectedSale.ean, expectedSale.sku, expectedSale.ufTarget.get, expectedSale.quantity),
        Row(expectedSale.warehouseCnpj, "3", beforeYesterday,
          expectedSale.ean, expectedSale.sku, expectedSale.ufTarget.get, 5D)
      )

      val salesDF = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val salesSeq = salesService.salesTransformer(salesDF, DateTime.now.minusDays(1), DateTime.now)

      salesSeq.sales.size shouldEqual (1)
      salesSeq.sales(0).quantity shouldEqual (3)

    }

  }

}
