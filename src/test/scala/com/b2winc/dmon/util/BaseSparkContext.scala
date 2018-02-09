package com.b2winc.dmon.util

import com.b2winc.dmon.web.WebServer
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait BaseSparkContext {

  private val master = "local[*]"
  private val appName = "spark-testing"

  protected implicit var sc: SparkContext = _
  protected implicit var sqlContext: HiveContext = _

  def startContext() {
    //stops the sparkContext used in production to avoid multiple contexts
    if(WebServer.sc != null) WebServer.sc.stop
    if(sc == null) {
      val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)

      sc = new SparkContext(conf)
    }

    if(sqlContext == null) {
      sqlContext = new HiveContext(sc)
    }
  }

  def stopContext(): Unit = {
    if(sc != null) sc.stop()
  }

  object SparkContextHelperTest extends SparkContextHelperIfc {
    override def createSparkContext: SparkContext = sc
    override def createSqlContext: HiveContext = sqlContext
  }

}
