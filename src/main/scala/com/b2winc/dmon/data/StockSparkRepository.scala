package com.b2winc.dmon.data
import java.sql.Timestamp

import com.b2winc.dmon.util.ConfigurationContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, types}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object StockSparkRepository {
  var stockDF: Option[DataFrame] = Option.empty
}

class StockSparkRepository extends SparkRepository {

  var databaseName = "datalake_umbrella."

  override def collector()(implicit sqlContext: HiveContext): DataFrame = {
    import sqlContext.implicits._

    sqlContext.refreshTable(s"${databaseName}umbrella_posicao_estoque")
    val sql =
      s"""
         |select
         |        est.id_item as sku,
         |        fil.fili_cgc as cnpj,
         |        est.partition_date as stockDate,
         |        est.load_timestamp as receivedDate,
         |        cast(est.qtde_disponivel as double) as available,
         |        est.id_agrup_valorizacao as brandGroup,
         |        est.id_cia as idCia
         |from ${databaseName}umbrella_posicao_estoque est
         |join ${databaseName}umbrella_filial fil
         |        on fil.fili_id_filial = est.id_filial
         |join ${databaseName}umbrella_item_geral iteg
         |        on est.id_item = iteg.iteg_id
         |        and iteg.iteg_id_fornec in ('280273002938', '280273000137', '280273000218', '280273000722')
         |where
         |        est.id_cia = '1'
         |        and est.id_agrup_valorizacao = '1'
      """.stripMargin

    val groupedStocks = groupStocksByDay(sqlContext.sql(sql))(sqlContext)
    val groupedWithAllDays = inputMissingDays(groupedStocks)
    inputMissingAvailability(groupedWithAllDays)
  }

  def inputMissingAvailability(groupedStocks: DataFrame)(implicit sqlContext: HiveContext) = {
    import sqlContext.implicits._

    def getKey(row: Row): String = s"${row.getAs[String]("sku")}${row.getAs[String]("cnpj")}${row.getAs[String]("brandGroup")}"
    def isValidDouble(value: Any): Boolean = {
      value match {
        case x: Double => !x.isNaN
        case null => false
        case _ => false
      }
    }

    val repartitioned = groupedStocks
      .repartition($"sku", $"brandGroup", $"cnpj")
      .sortWithinPartitions(asc("sku"), asc("stockDate"), asc("brandGroup"), asc("cnpj"))

    implicit val encoder = RowEncoder(groupedStocks.schema)
    repartitioned.mapPartitions{ partitionRows: Iterator[Row] =>
      var res = List[Row]()
      val rowsIterator = partitionRows
      var mapWithLastFound = Map[String, Double]()
      while(rowsIterator.hasNext) {
        val row = rowsIterator.next()
        val currentKey = getKey(row)
        val rowAvailableAny = row.getAs[AnyRef]("available")
        if (!isValidDouble(rowAvailableAny)) {
          val lastFound = mapWithLastFound.get(currentKey)
          if (lastFound.isDefined)
            res = res.+:(Row(row.getAs[String]("sku"), row.getAs[String]("cnpj"), row.getAs[String]("stockDate"), row.getAs[Timestamp]("receivedDate"),
              lastFound.get, row.getAs[String]("brandGroup"), row.getAs[String]("idCia")))
        }
        else {
          res = res.+:(row)
          mapWithLastFound = mapWithLastFound + (currentKey -> rowAvailableAny.asInstanceOf[Double])
        }
      }
      res.iterator
    }
  }

  def inputMissingDays(groupedStocks: DataFrame)(implicit sqlContext: HiveContext) = {
    sqlContext.setConf("spark.sql.crossJoin.enabled", "true")
    val lastXDays = getPastXDays()
    val lastXDaysDF = createDaysDataFrame(lastXDays)
    val distinctItems = groupedStocks.select("sku", "brandGroup", "cnpj", "idCia").distinct()

    //cartesian
    val stocksNeeded = distinctItems.join(lastXDaysDF)
    stocksNeeded
      .join(groupedStocks, Seq("sku", "brandGroup", "cnpj", "idCia", "stockDate"), "left_outer")
      .select("sku", "cnpj", "stockDate", "receivedDate", "available", "brandGroup", "idCia")
  }

  def createDaysDataFrame(days: Seq[String])(implicit sqlContext: HiveContext) = {
    val schema = StructType(Array(
      StructField("stockDate", types.StringType)
    ))
    val rows = days.map(day => Row(day))
    sqlContext.createDataFrame(sqlContext.sparkSession.sparkContext.parallelize(rows), schema)
  }

  def getPastXDays(): Seq[String]= {
    val today = DateTime.now
    val end = 30 * ConfigurationContext.getIntConfigOrDefault("monthsBackOnPartition", 1)
    (0 to end).map{ i =>
      today.minusDays(i).toString("yyyy-MM-dd")
    }
  }

  def groupStocksByDay(stockDF: DataFrame)(implicit sqlContext: HiveContext): DataFrame = {
    import sqlContext.implicits._

    val groupedStocks = stockDF
      .groupBy("cnpj", "brandGroup", "sku", "stockDate")
      .agg("receivedDate" -> "max")
      .alias("gs")

    groupedStocks.join(stockDF.alias("st"),
      $"gs.cnpj" === $"st.cnpj" &&
        $"gs.brandGroup" === $"st.brandGroup" &&
        $"gs.sku" === $"st.sku" &&
        $"gs.stockDate" === $"st.stockDate" &&
        $"gs.max(receivedDate)" === $"st.receivedDate")
      .select($"gs.cnpj", $"gs.brandGroup", $"gs.sku", $"gs.stockDate", $"st.receivedDate", $"st.available", $"st.idCia")
  }

  override def getCached()(implicit sqlContext: HiveContext): Option[DataFrame] = {
    if(StockSparkRepository.stockDF.isEmpty) StockSparkRepository.stockDF = Some(
      collector()(sqlContext)
        .coalesce(4)
        .cache()
    )
    StockSparkRepository.stockDF
  }

  override def updateCached()(implicit sqlContext: HiveContext): Unit = {
    if(StockSparkRepository.stockDF.isDefined)
      StockSparkRepository.stockDF.get.unpersist()
    StockSparkRepository.stockDF = Some(collector()(sqlContext).cache())
  }

  def updateTmpTable()(implicit sqlContext: HiveContext): Unit = {
    val today = DateTime.now.toString("yyyy-MM-dd")
    sqlContext.sql(
      s"""
        |insert overwrite datalake_umbrella.umbrella_posicao_estoque_temp partition (partition_date)
        |select * from datalake_umbrella.umbrella_posicao_estoque where partition_date < '$today'
      """.stripMargin).collect()
  }


}
