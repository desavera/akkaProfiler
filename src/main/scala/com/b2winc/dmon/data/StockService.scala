package com.b2winc.dmon.data

import java.sql.Timestamp

import com.b2winc.dmon.model.DataNotAvailableException
import com.b2winc.dmon.util.ConfigurationContext
import grizzled.slf4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class Stock(warehouseCnpj: String, brandGroup: String, stockDate: String, ean: String, sku: String, availableStock: Double)
case class Stocks(stocks: Seq[Stock])
class StockService(stockRepository: StockSparkRepository,
                   itemsSparkRepository: ItemsSparkRepository,
                   toolboxService: ToolboxService) extends Serializable {

  lazy val logger = Logger(getClass())

  def stockTransformer(stockDF: DataFrame, itemsDF: DataFrame, initialDate: DateTime, finalDate: DateTime)(implicit sqlContext: HiveContext) = {
    import sqlContext.implicits._

    val itemsWithStock = stockDF
      .filter($"stockDate" >= ConfigurationContext.minimumDateAsString)
      .filter($"stockDate" >= initialDate.toString("yyyy-MM-dd"))
      .filter($"stockDate" <= finalDate.toString("yyyy-MM-dd"))
      .filter($"available".isNotNull)
      .join(itemsDF,
        $"sku" === $"iteg_id" &&
        $"idCia" === $"iteg_id_cia")

    Stocks(stocks =
      itemsWithStock.map { row =>
        Stock(warehouseCnpj = row.getAs[String]("cnpj"),
          stockDate = row.getAs[String]("stockDate"),
          ean = row.getAs[String]("iteg_ean"),
          sku = row.getAs[String]("sku"),
          brandGroup = row.getAs[String]("brandGroup"),
          availableStock = row.getAs[Double]("available"))
      }.collect
    )
  }



  def getStock(initialDate: Option[String], finalDate: Option[String])(ec: ExecutionContext)(implicit sqlContext: HiveContext): Future[Stocks] = {

    val (initialDateDT, finalDateDT) = toolboxService.validateAndConvertDates(initialDate, finalDate)

    val stockDFTry = Try(stockRepository.getCached()(sqlContext))
    val itemsDFTry = Try(itemsSparkRepository.getCached()(sqlContext))

    stockDFTry match {
      case Success(stock) => {
        itemsDFTry match {
          case Success(items) => Future(stockTransformer(stock.get, items.get, initialDateDT, finalDateDT)(sqlContext))(ec)
          case Failure(e) => {
            logger.error("Failed to get data.", e)
            throw new DataNotAvailableException(e.getMessage)
          }
        }
      }
      case Failure(e) => {
        logger.error("Failed to get data.", e)
        throw new DataNotAvailableException(e.getMessage)
      }
    }
  }
}
