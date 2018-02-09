package com.b2winc.dmon.data
import com.b2winc.dmon.util.ConfigurationContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime


object ShipmentSparkRepository {
  //the DF is initialized empty, at the first call this cache var will be populated
  var shipmentDF: Option[DataFrame] = Option.empty
}

class ShipmentSparkRepository extends SparkRepository {

  override def collector()(implicit sqlContext: HiveContext): DataFrame = {
    val monthsBack = ConfigurationContext.getIntConfigOrDefault("monthsBackOnPartition", 1)
    val dateLimit = DateTime.now.minusMonths(monthsBack).toString("yyyy-MM")

    val database = "datalake_wms"
    val pedidoCab = s"$database.wms_pedido_cab"
    val planta = s"$database.wms_planta"
    val pedidoCabEtic = s"$database.wms_pedido_cab_etic"
    val pedidoDet = s"$database.wms_pedido_det"
    val item = s"$database.wms_item"
    val ean = s"$database.wms_ean"
    val numeroSerie = s"$database.wms_numero_serie"

    sqlContext.refreshTable(pedidoCab)
    sqlContext.refreshTable(planta)
    sqlContext.refreshTable(pedidoCabEtic)
    sqlContext.refreshTable(pedidoDet)
    sqlContext.refreshTable(item)
    sqlContext.refreshTable(ean)
    sqlContext.refreshTable(numeroSerie)

    val sql =
      s"""
        select
        |    cab.pedv_id_unineg as brand,
        |    pla.plta_id_terceiro as warehouseCnpj,
        |    pla.plta_id_estado as warehouseUf,
        |    etic.pcae_muni_nome as deliveryCity,
        |    etic.pcae_id_estado as deliveryUf,
        |    item.item_cod_terceiro as sku,
        |    ean.ean_id_ean as ean,
        |    ns.nuse_nu_serie as imei,
        |    cab.pedv_dt_situacao as dispatchDate,
        |    cab.pedv_dt_emissao as salesDate,
        |    etic.pcae_data_faturamento as nfIssuance,
        |    etic.pcae_num_nota as nfId,
        |    etic.pcae_serie as nfSerie
        |from
        |$pedidoCab cab
        |join $planta pla
        |    on cab.pedv_id_planta = pla.plta_id_planta
        |join $pedidoCabEtic etic
        |    on cab.pedv_id_ped = etic.pcae_id_ped
        |join $pedidoDet det
        |    on cab.pedv_id_ped = det.pedd_id_ped
        |join $item item
        |    on det.pedd_id_item = item.item_id_item
        |join $ean ean
        |    on item.item_id_sku = ean.ean_id_sku
        |join $numeroSerie ns
        |    on ns.nuse_id_programa = cab.pedv_id_programa
        |    and ns.nuse_id_onda = cab.pedv_id_onda
        |    and ns.nuse_id_prevol = cab.pedv_id_prevol
        |    and cab.pedv_situacao = 'L'
        |join datalake_umbrella.umbrella_item_geral iteg
        |  on item.item_cod_terceiro = iteg.iteg_id
        |  and iteg.iteg_id_cia = '1'
        |where
        |    item.item_id_fornec in ('280273002938', '280273000137', '280273000218', '280273000722')
        |    and cab.pedv_id_unineg in ('1', '29', '26')
        |    and cab.pedv_dt_registro_p > '$dateLimit'
        |    and etic.pcae_datahora_p > '$dateLimit'
        |    and det.pedv_dt_registro_p > '$dateLimit'
        |    and ns.volu_dt_registro_p > '$dateLimit'
        |    and iteg.iteg_id_depto = '18'
      """.stripMargin

  sqlContext.sql(sql)
  }

  override def getCached()(implicit sqlContext: HiveContext): Option[DataFrame] = {
    if(ShipmentSparkRepository.shipmentDF.isEmpty)
      ShipmentSparkRepository.shipmentDF = Some(collector()(sqlContext).cache())
    ShipmentSparkRepository.shipmentDF
  }

  override def updateCached()(implicit sqlContext: HiveContext): Unit = {
    try{
      if(ShipmentSparkRepository.shipmentDF.isDefined)
        ShipmentSparkRepository.shipmentDF.get.unpersist()
      ShipmentSparkRepository.shipmentDF = Some(collector()(sqlContext).cache())
    } finally {

    }
  }

}
