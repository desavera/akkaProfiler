package com.b2winc.dmon.data

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, _}

trait SparkRepository extends Serializable {

  def collector()(implicit sqlContext: HiveContext): DataFrame = ???
  def getCached()(implicit sqlContext: HiveContext): Option[DataFrame] = ???
  def updateCached()(implicit sqlContext: HiveContext): Unit = ???

}
