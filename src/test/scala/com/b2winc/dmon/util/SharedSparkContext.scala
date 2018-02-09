package com.b2winc.dmon.util

import org.scalatest.{BeforeAndAfterAll, WordSpec}

trait SharedSparkContext extends WordSpec with BeforeAndAfterAll with BaseSparkContext {

  override protected def beforeAll: Unit = {
    startContext()
    super.beforeAll()
  }

  override protected def afterAll: Unit = {
    stopContext()
    super.afterAll()
  }
  /*override def afterAll: Unit = {
    stopContext()
    super.afterAll
  }*/
}
