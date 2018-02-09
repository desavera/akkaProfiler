package com.b2winc.dmon.data

import com.b2winc.dmon.util.AsyncSharedSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, _}
import org.joda.time.DateTime
import org.scalamock.scalatest.{AsyncMockFactory, MockFactory}
import org.scalatest.Matchers

import scala.concurrent.{ExecutionContext, Future}

class SalesServiceAsyncTest extends AsyncSharedSparkContext with AsyncMockFactory with Matchers {

  val salesRepository = new SalesSparkRepository
  val toolboxService = new ToolboxService
  val salesService = new SalesService(salesRepository, toolboxService)

  "The getSales" should {

    implicit val ec = ExecutionContext.global

    "fail gracefully when required parameters are not specified" in {

      val empty: Option[String] = Option.empty
      val mockDate: Option[String] = Some("2011-01-01")
      val emptyString: Option[String] = Some("")

      def assertGetSalesException(initialDate: Option[String], finalDate: Option[String], missing: String) = {
        val thrown = the [IllegalArgumentException] thrownBy salesService.getSales(initialDate, finalDate)(ec)
        thrown.getMessage should equal (s"requirement failed: The $missing was not informed.")
      }

      assertGetSalesException(empty, mockDate, "initialDate")
      assertGetSalesException(mockDate, empty, "finalDate")
      assertGetSalesException(empty, empty, "initialDate")
      assertGetSalesException(emptyString, mockDate, "initialDate")
      assertGetSalesException(mockDate, emptyString, "finalDate")
      succeed
    }

    "fail gracefully when the date is not in the specified format" in {
      val invalidFormatDate = Some(DateTime.now.toString("dd-MM-yyyy"))
      val validFormatDate = Some(DateTime.now.toString("yyyy-MM-dd"))

      def assertGetSalesException(initialDate: Option[String], finalDate: Option[String], wrongFormat: Option[String]) = {
        val thrown = the [IllegalArgumentException] thrownBy salesService.getSales(initialDate, finalDate)(ec)
        thrown.getMessage should equal (s"The date ${wrongFormat.get} was not specified in a valid format (YYYY-MM-DD).")
      }
      assertGetSalesException(invalidFormatDate, validFormatDate, Some(s"${invalidFormatDate.get} 00:00"))
      assertGetSalesException(validFormatDate, invalidFormatDate, Some(s"${invalidFormatDate.get} 23:59"))

    }

    "return a seq of sales in the happy path" in {

      //setup
      val sale = stub[Sale]
      val expectedResult = Sales(sales=Array(sale))
      val mockedSalesRepository = mock[SalesSparkRepository]
      val mockedToolboxService = mock[ToolboxService]

      //since scalamock do not support partial mock, it's necessary to manually mock the return of the salesTransformer method
      class MSalesService extends SalesService(mockedSalesRepository, mockedToolboxService) {
        //mocking the behavior of this method
        override def salesTransformer(salesDF: DataFrame, initialDate: DateTime, finalDate: DateTime)(implicit sqlContext: HiveContext): Sales = {
          return expectedResult
        }
      }

      val salesService = new MSalesService

      //mock a dataframe in order to mimic a database call
      val dfSchema = StructType(
        Array(
          StructField("test", StringType)
        )
      )
      val rows = Array(
        Row("")
      )
      val mockedDataframe = sqlContext.createDataFrame(sc.parallelize(rows), dfSchema)

      val initialDate = DateTime.now.minusDays(15)
      val finalDate = DateTime.now

      (mockedToolboxService.validateAndConvertDates _)
        .expects(Some(initialDate.toString("yyyy-MM-dd")), Some(finalDate.toString("yyyy-MM-dd")))
        .returning((initialDate, finalDate))

      (mockedSalesRepository.getCached()(_: HiveContext))
        .expects(sqlContext)
        .returning(Some(mockedDataframe))

      val futureResponse: Future[Sales] = salesService.getSales(
        Some(initialDate.toString("yyyy-MM-dd")),
        Some(finalDate.toString("yyyy-MM-dd")))(ec)(sqlContext)

      futureResponse map {sales => sales shouldEqual expectedResult}
    }

    "fail gracefully when the informed date is greater than 1 month" in {
      val initialDate = Some(DateTime.now.minusMonths(2).toString("yyyy-MM-dd"))
      val finalDate = Some(DateTime.now.toString("yyyy-MM-dd"))
      val thrown = the [IllegalArgumentException] thrownBy salesService.getSales(initialDate, finalDate)(ec)
      thrown.getMessage shouldEqual "The specified initialDate is older than 1 month."
    }
  }

}