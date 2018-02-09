package com.b2winc.dmon.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextHelperIfc {
  def createSparkContext: SparkContext = ???
  def createSqlContext: HiveContext = ???
}

object SparkContextHelper extends SparkContextHelperIfc{

  private var sc: Option[SparkContext] = Option.empty
  private var sqlContext: Option[HiveContext] = Option.empty

  override def createSparkContext: SparkContext = {
    if(!sc.isDefined) sc = Some(new SparkContext(new SparkConf().setMaster("local[*]").setAppName("dmon-api")))
    sc.get
  }

  override def createSqlContext: HiveContext = {
    if(!sqlContext.isDefined) sqlContext = Some(new HiveContext(sc.get))
    sqlContext.get
  }
}
