package com.b2winc.dmon.data

import java.sql
import java.sql.Timestamp

import com.b2winc.dmon.model.DataNotAvailableException
import grizzled.slf4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import com.b2winc.dmon.model.BrandConverters._
import com.b2winc.dmon.util.ConfigurationContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class Sale(warehouseCnpj: String, ufTarget: Option[String], brand: String, salesDate: sql.Timestamp, ean: String,
                sku: String, quantity: Double)
case class Sales(sales: Seq[Sale])

class SalesService(salesRepository: SalesSparkRepository, toolboxService: ToolboxService) extends Serializable{

  lazy val logger = Logger(getClass)
  //This method receives a salesDF, do the necessary manipulations, and return a Seq of Sale
  def salesTransformer(salesDF: DataFrame, initialDate: DateTime, finalDate: DateTime)(implicit sqlContext: HiveContext): Sales = {
    import sqlContext.implicits._

    val groupedSales = salesDF
      .filter($"salesDate" >= ConfigurationContext.minimumDate)
      .filter($"salesDate" >= new Timestamp(initialDate.getMillis))
      .filter($"salesDate" <= new Timestamp(finalDate.getMillis))
      .groupBy("cnpj", "brand", "salesDate", "ean", "sku", "ufTarget")
      .agg("quantity" -> "sum")

    val result = Sales(sales =
      groupedSales.map { row =>
        Sale(warehouseCnpj = row.getAs[String]("cnpj"), ufTarget = Some(row.getAs[String]("ufTarget")),
          brand = row.getAs[String]("brand").asBrandUmbrella, salesDate = row.getAs[sql.Timestamp]("salesDate"),
          ean = row.getAs[String]("ean"), sku = row.getAs[String]("sku"), quantity = row.getAs[Double]("sum(quantity)"))
      }.collect
    )
    result
  }

  def getSales(initialDate: Option[String], finalDate: Option[String])(ec: ExecutionContext)(implicit sqlContext: HiveContext): Future[Sales] = {

    val (initialDateDT, finalDateDT) = toolboxService.validateAndConvertDates(initialDate, finalDate)

    val salesDFTry = Try(salesRepository.getCached()(sqlContext))
    salesDFTry match {
      case Success(v) => Future(salesTransformer(v.get, initialDateDT, finalDateDT)(sqlContext))(ec)
      case Failure(e) => {
        logger.error("Failed to get data.", e)
        throw new DataNotAvailableException(e.getMessage)
      }
    }
  }

}