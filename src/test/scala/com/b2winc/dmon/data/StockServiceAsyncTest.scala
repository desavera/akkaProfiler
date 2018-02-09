package com.b2winc.dmon.data

import com.b2winc.dmon.util.AsyncSharedSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, _}
import org.joda.time.DateTime
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Matchers

import scala.concurrent.{ExecutionContext, Future}

class StockServiceAsyncTest extends AsyncSharedSparkContext with AsyncMockFactory with Matchers {

  val stockRepository = new StockSparkRepository
  val toolboxService = new ToolboxService
  val itemRepository = new ItemsSparkRepository
  val stockService = new StockService(stockRepository, itemRepository, toolboxService)


  "the getStock" should {
    implicit val ec = ExecutionContext.global

    "fail gracefully when required parameters are not specified" in {
      val empty: Option[String] = Option.empty
      val mockDate: Option[String] = Some("2011-01-01")
      val emptyString: Option[String] = Some("")

      def assertGetStockException(initialDate: Option[String], finalDate: Option[String], missing: String) = {
        val thrown = the [IllegalArgumentException] thrownBy stockService.getStock(initialDate, finalDate)(ec)
        thrown.getMessage shouldEqual (s"requirement failed: The $missing was not informed.")
      }

      assertGetStockException(empty, mockDate, "initialDate")
      assertGetStockException(mockDate, empty, "finalDate")
      assertGetStockException(empty, empty, "initialDate")
      assertGetStockException(emptyString, mockDate, "initialDate")
      assertGetStockException(mockDate, emptyString, "finalDate")
    }

    "fail gracefully when the date is not in the specified format" in {
      val invalidFormatDate = Some(DateTime.now.toString("dd-MM-yyyy"))
      val validFormatDate = Some(DateTime.now.toString("yyyy-MM-dd"))

      def assertGetStockException(initialDate: Option[String], finalDate: Option[String], wrongFormat: Option[String]) = {
        val thrown = the [IllegalArgumentException] thrownBy stockService.getStock(initialDate, finalDate)(ec)
        thrown.getMessage shouldEqual (s"The date ${wrongFormat.get} was not specified in a valid format (YYYY-MM-DD).")
      }

      assertGetStockException(invalidFormatDate, validFormatDate, Some(s"${invalidFormatDate.get} 00:00"))
      assertGetStockException(validFormatDate, invalidFormatDate, Some(s"${invalidFormatDate.get} 23:59"))
    }

    "return Stocks in the happy path" in {

      val stock = mock[Stock]
      val expectedResult = Stocks(stocks=Array(stock))
      val mockedStockRepository = mock[StockSparkRepository]
      val mockedItemRepository = mock[ItemsSparkRepository]
      val mockedToolboxService = mock[ToolboxService]

      //since scalamock do not support partial mock, it's necessary to manually mock the return of the stockTransformer method
      class MStockService extends StockService(mockedStockRepository, mockedItemRepository, mockedToolboxService) {
        override def stockTransformer(stockDF: DataFrame, itemsDF:DataFrame, initialDate: DateTime, finalDate: DateTime)(implicit sqlContext: HiveContext): Stocks = {
          return expectedResult
        }
      }

      val stockService = new MStockService

      //mock a dataframe in order to mimic a database call
      val dfSchema = StructType(
        Array(
          StructField("test", StringType)
        )
      )
      val rows = Array(
        Row("")
      )

      val initialDate = DateTime.now.minusDays(15)
      val finalDate = DateTime.now

      val mockedDataframe = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      (mockedToolboxService.validateAndConvertDates _)
        .expects(Some(initialDate.toString("yyyy-MM-dd")), Some(finalDate.toString("yyyy-MM-dd")))
        .returning((initialDate, finalDate))

      (mockedStockRepository.getCached()(_: HiveContext))
        .expects(sqlContext)
        .returning(Some(mockedDataframe))

      (mockedItemRepository.getCached()(_: HiveContext))
        .expects(sqlContext)
        .returning(Some(mockedDataframe))

      val futureResponse: Future[Stocks] = stockService.getStock(
        Some(initialDate.toString("yyyy-MM-dd")),
        Some(finalDate.toString("yyyy-MM-dd")))(ec)(sqlContext)

      futureResponse map (stocks => stocks shouldEqual expectedResult)
    }

    "fail gracefully when the informed date is greater than 1 month" in {
      val initialDate = Some(DateTime.now.minusMonths(2).toString("yyyy-MM-dd"))
      val finalDate = Some(DateTime.now.toString("yyyy-MM-dd"))
      val thrown = the [IllegalArgumentException] thrownBy stockService.getStock(initialDate, finalDate)(ec)
      thrown.getMessage shouldEqual "The specified initialDate is older than 1 month."
    }

  }
}
