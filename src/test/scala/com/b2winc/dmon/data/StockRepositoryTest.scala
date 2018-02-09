package com.b2winc.dmon.data

import java.sql.Timestamp

import com.b2winc.dmon.util.SharedSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers

class StockRepositoryTest extends SharedSparkContext with MockFactory with Matchers {

  val stockRepository = new StockSparkRepository

  val expectedStock = Stock(sku = "1", warehouseCnpj = "1", ean = "1234", stockDate = DateTime.now.toString("yyyy-MM-dd"),
    availableStock = 30, brandGroup = "1")

  val dateNow = new Timestamp(DateTime.now.getMillis)
  val datePastOneHour = new Timestamp(DateTime.now.minusHours(1).getMillis)

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

  "the method groupStocksByDay" should {

    "group by max date" in {
      val datePastOneHour = new Timestamp(DateTime.now.minusHours(1).getMillis)
      val dateTmrrw = new Timestamp(DateTime.now.plusDays(1).getMillis)
      val dateTmrrwPast = new Timestamp(DateTime.now.minusHours(3).plusDays(1).getMillis)

      val tomorrowDateStr = DateTime.now.plusDays(1).toString("yyyy-MM-dd")

      val rows = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, datePastOneHour,
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, tomorrowDateStr, dateTmrrw,
          12D, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, tomorrowDateStr, dateTmrrwPast,
          15D, expectedStock.brandGroup, "1")
      )

      val mockedDataFrame = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val df = stockRepository.groupStocksByDay(mockedDataFrame)(sqlContext)

      df.count() shouldEqual 2
      val resultedRows = df.orderBy(asc("stockDate")).collect()
      resultedRows(0).getAs[String]("cnpj") shouldEqual expectedStock.warehouseCnpj
      resultedRows(0).getAs[String]("stockDate") shouldEqual expectedStock.stockDate
      resultedRows(0).getAs[Double]("available") shouldEqual 30D

      resultedRows(1).getAs[String]("cnpj") shouldEqual expectedStock.warehouseCnpj
      resultedRows(1).getAs[String]("stockDate") shouldEqual tomorrowDateStr
      resultedRows(1).getAs[Double]("available") shouldEqual 12D
    }

    "aggregate rows and return most recent available stock when the DataFrame has several rows" in {
      val rows = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, new Timestamp(DateTime.now.plusHours(2).getMillis),
          10D, expectedStock.brandGroup, "1")
      )

      val mockedDataFrame = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val df = stockRepository.groupStocksByDay(mockedDataFrame)(sqlContext)

      val resultRows = df.collect()
      resultRows(0).getAs[Double]("available") shouldEqual 10D
    }

    "group by expected fields" in {

      val dateNow = new Timestamp(DateTime.now.getMillis)

      val rows = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row("2", expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          20D, expectedStock.brandGroup, "1")
      )

      val mockedDataFrame = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val stocks = stockRepository.groupStocksByDay(mockedDataFrame)(sqlContext)

      //make sure it did not grouped by different sku
      stocks
        .orderBy(asc("sku"))
        .collect()(0).getAs[Double]("available") shouldEqual (expectedStock.availableStock)
      stocks
        .orderBy(asc("sku"))
        .collect()(1).getAs[Double]("available") shouldEqual 20D

      val differentCnpjRow = Array(
        Row(expectedStock.sku, "2", expectedStock.stockDate, dateNow, 21D, expectedStock.brandGroup, "1")
      )

      val newRowCnpj = sqlContext.createDataFrame(sc.parallelize(differentCnpjRow), dfSchema)

      val stocksCnpj = stockRepository.groupStocksByDay(mockedDataFrame.union(newRowCnpj))(sqlContext)
      //make sure it did not grouped by different cnpj
      stocksCnpj
        .orderBy(asc("sku"), asc("cnpj"))
        .collect()(0).getAs[Double]("available") shouldEqual (expectedStock.availableStock)
      stocksCnpj
        .orderBy(asc("sku"), asc("cnpj"))
        .collect()(1).getAs[Double]("available") shouldEqual 21D

      val differentBrandRow = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow, 23D, "4", "1")
      )

      val newRowBrand = sqlContext.createDataFrame(sc.parallelize(differentBrandRow), dfSchema)

      val stocksBrand = stockRepository.groupStocksByDay(mockedDataFrame.union(newRowBrand))(sqlContext)
      //make sure it did not grouped by different brand
      stocksBrand
        .orderBy(asc("sku"), asc("brandGroup"))
        .collect()(0).getAs[Double]("available") shouldEqual (expectedStock.availableStock)
      stocksBrand
        .orderBy(asc("sku"), asc("brandGroup"))
        .collect()(1).getAs[Double]("available") shouldEqual 23D
    }

  }

  "the method getPast30Days" should {
    "return a SEQ with strings of days" in {
      val last30Days = stockRepository.getPastXDays()
      val today = DateTime.now
      (0 to 30).map{ i =>
        last30Days should contain (today.minusDays(i).toString("yyyy-MM-dd"))
      }
    }
  }

  "the method inputMissingDays" should {
    "return a new dataframe with all days on the last month" in {

      val implicits = sqlContext.implicits

      val rows = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, datePastOneHour,
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row("321", expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          40D, expectedStock.brandGroup, "1")
      )

      val df = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)
      val groupedDf = stockRepository.groupStocksByDay(df)
      val result = stockRepository.inputMissingDays(groupedDf)

      import implicits._

      result.schema shouldEqual dfSchema

      result.count() shouldEqual 62

      val t1 = result
        .filter($"stockDate" === expectedStock.stockDate
        && $"sku" === expectedStock.sku)
      t1.count() shouldEqual 1
      t1.collect()(0).getAs[Double]("available") shouldEqual expectedStock.availableStock

      val t2 = result
        .filter($"stockDate" === expectedStock.stockDate
          && $"sku" === "321")

      t2.count() shouldEqual 1
      t2.collect()(0).getAs[Double]("available") shouldEqual 40D

      val t3 = result
        .filter($"stockDate" === DateTime.now.minusDays(2).toString("yyyy-MM-dd")
          && $"sku" === expectedStock.sku)
      t3.count() shouldEqual 1
      t3.collect()(0).getAs[AnyRef]("available") shouldEqual null

    }
  }

  "the method inputMissingAvailability" should {
    "input the last available stock for each product" in {

      val implicits = sqlContext.implicits

      val yesterdayDt = DateTime.now.minusDays(1)
      val beforeYesterdayDt = DateTime.now.minusDays(2)

      val rows = Array(
        Row(expectedStock.sku, expectedStock.warehouseCnpj, beforeYesterdayDt.toString("yyyy-MM-dd"), new Timestamp(beforeYesterdayDt.getMillis),
          expectedStock.availableStock, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, yesterdayDt.toString("yyyy-MM-dd"), new Timestamp(yesterdayDt.getMillis),
          null, expectedStock.brandGroup, "1"),
        Row(expectedStock.sku, expectedStock.warehouseCnpj, expectedStock.stockDate, dateNow,
          null, expectedStock.brandGroup, "1")
      )

      val df = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)
      val filledUp = stockRepository.inputMissingAvailability(df)

      import implicits._

      val availablesRows = filledUp.filter($"sku" === expectedStock.sku).collect()
      for (row <- availablesRows)
         row.getAs[Double]("available") shouldEqual expectedStock.availableStock
   }
  }

  "the method inputMissingAvailability" should {
  "fill up the last available stock for each products from different partitions" in {

    val implicits = sqlContext.implicits

    val inputStock1 = Stock(sku = "1", warehouseCnpj = "1", ean = "1234", stockDate = DateTime.now.toString("yyyy-MM-dd"),
      availableStock = 30, brandGroup = "1")
    val inputStock2 = Stock(sku = "2", warehouseCnpj = "1", ean = "1234", stockDate = DateTime.now.toString("yyyy-MM-dd"),
      availableStock = 40, brandGroup = "1")

    // +1 day fillup mocks
    val inputStock12 = Stock(sku = "1", warehouseCnpj = "1", ean = "1234", stockDate = DateTime.now.plusDays(1).toString("yyyy-MM-dd"),
      availableStock = Double.NaN, brandGroup = "1")
    val inputStock22 = Stock(sku = "2", warehouseCnpj = "1", ean = "1234", stockDate = DateTime.now.plusDays(1).toString("yyyy-MM-dd"),
      availableStock = Double.NaN, brandGroup = "1")

    val rows = Array(
      Row(inputStock1.sku, inputStock1.warehouseCnpj, inputStock1.stockDate, dateNow,
        inputStock1.availableStock, inputStock1.brandGroup, "1"),

      Row(inputStock2.sku, inputStock2.warehouseCnpj, inputStock2.stockDate, dateNow,
        inputStock2.availableStock, inputStock2.brandGroup, "1"),

      Row(inputStock12.sku, inputStock12.warehouseCnpj, inputStock12.stockDate, null,
        inputStock12.availableStock, inputStock12.brandGroup, "1"),

      Row(inputStock22.sku, inputStock22.warehouseCnpj, inputStock22.stockDate, null,
        inputStock22.availableStock, inputStock22.brandGroup, "1")
    )

      val df = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)
      val filledUp = stockRepository.inputMissingAvailability(df)

      import implicits._

      val availables1 = filledUp.filter($"sku" === "1").collect()
      for (row <- availables1)
         row.getAs[Double]("available") shouldEqual 30

      val availables2 = filledUp.filter($"sku" === "2").collect()
      for (row <- availables2)
         row.getAs[Double]("available") shouldEqual 40

    }
   }

  "the method collector" should {
    "return a valid dataframe without missing days" in {

      val dfEstoque = sqlContext
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(getClass.getResource("/data/posicao_estoque.csv").toString)

      val dfItem = sqlContext
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(getClass.getResource("/data/item.csv").toString)

      val dfFilial = sqlContext
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(getClass.getResource("/data/filial.csv").toString)

      dfEstoque.createOrReplaceTempView("umbrella_posicao_estoque")
      dfItem.createOrReplaceTempView("umbrella_item_geral")
      dfFilial.createOrReplaceTempView("umbrella_filial")

      stockRepository.databaseName = ""
      stockRepository.collector().show
    }
  }
}
