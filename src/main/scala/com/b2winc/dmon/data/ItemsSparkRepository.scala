package com.b2winc.dmon.data
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object ItemsSparkRepository{
  var itemsDF: Option[DataFrame] = Option.empty
}

class ItemsSparkRepository extends SparkRepository{

  override def collector()(implicit sqlContext: HiveContext): DataFrame = {
    val sql =
      """
        |select * from datalake_umbrella.umbrella_item_geral where iteg_id_fornec in ('280273002938', '280273000137', '280273000218', '280273000722')
      """.stripMargin

    sqlContext.sql(sql)
  }

  override def getCached()(implicit sqlContext: HiveContext): Option[DataFrame] = {
    if(ItemsSparkRepository.itemsDF.isEmpty)
      ItemsSparkRepository.itemsDF = Some(
      collector()(sqlContext).cache()
    )
    ItemsSparkRepository.itemsDF
  }

  override def updateCached()(implicit sqlContext: HiveContext): Unit = {
    if(ItemsSparkRepository.itemsDF.isDefined)
      ItemsSparkRepository.itemsDF.get.unpersist()
    ItemsSparkRepository.itemsDF = Some(collector()(sqlContext).cache())
  }
}
