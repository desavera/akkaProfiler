package com.b2winc.dmon.data

import com.b2winc.dmon.util.ConfigurationContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, _}
import org.joda.time.DateTime

object SalesSparkRepository {
  //the DF is initialized empty, at the first call this cache var will be populated
  var salesDF: Option[DataFrame] = Option.empty
}

class SalesSparkRepository extends SparkRepository {

  //This method will collect raw data in the datasource
  override def collector()(implicit sqlContext: HiveContext): DataFrame = {
    val monthsBack = ConfigurationContext.getIntConfigOrDefault("monthsBackOnPartition", 1)
    val dateLimit = DateTime.now.minusMonths(monthsBack).toString("yyyy-MM")
    sqlContext.refreshTable("datalake_umbrella.umbrella_filial")
    sqlContext.refreshTable("datalake_umbrella.umbrella_pedido_de_venda_cabecalho")
    sqlContext.refreshTable("datalake_umbrella.umbrella_pedc_cliente")
    sqlContext.refreshTable("datalake_umbrella.umbrella_pedido_de_venda_detalhes")
    sqlContext.refreshTable("datalake_umbrella.umbrella_item_geral")
    val sql =
      s"""
        select cab.pedc_dt_emissao as salesDate,
         |cab.pedc_id_marca as brand,
         |det.pedd_id_item as sku,
         |fil.fili_cgc as cnpj,
         |ent.pece_id_estado as ufTarget,
         |iteg.iteg_ean as ean,
         |det.pedd_qt_ped as quantity
         |from datalake_umbrella.umbrella_pedido_de_venda_cabecalho cab
         |join datalake_umbrella.umbrella_pedc_cliente ent
         |on cab.pedc_id_pedido = ent.pece_id_pedido
         |and ent.pece_in_tipo = 'E'
         |join datalake_umbrella.umbrella_pedido_de_venda_detalhes det
         |on cab.pedc_id_pedido = det.pedd_id_pedido
         |left join datalake_umbrella.umbrella_item_geral iteg
         |on iteg.iteg_id = det.pedd_id_item
         |left join datalake_umbrella.umbrella_filial fil
         |on cab.pedc_id_filial = fil.fili_id_filial
         |where cab.pedc_situacao in ('A', 'L')
         |and pedc_id_marca in ('1','2','3')
         |and cab.pedc_in_aprovado = 'S'
         |and cab.pedc_id_origem = 'LJ'
         |and cab.pedc_id_cia = '1'
         |and iteg.iteg_id_fornec in ('280273002938', '280273000137', '280273000218', '280273000722')
         |and cab.data_inclusao_p >= '$dateLimit'
         |and ent.data_inclusao_p >= '$dateLimit'
         |and det.data_inclusao_p >= '$dateLimit'
      """.stripMargin

    sqlContext.sql(sql)
  }

  override def getCached()(implicit sqlContext: HiveContext): Option[DataFrame] = {
    if(SalesSparkRepository.salesDF.isEmpty) SalesSparkRepository.salesDF = Some(collector()(sqlContext).cache())
    SalesSparkRepository.salesDF
  }

  override def updateCached()(implicit sqlContext: HiveContext) = {
    if(SalesSparkRepository.salesDF.isDefined)
      SalesSparkRepository.salesDF.get.unpersist
    SalesSparkRepository.salesDF = Some(collector()(sqlContext).cache())
  }

}
