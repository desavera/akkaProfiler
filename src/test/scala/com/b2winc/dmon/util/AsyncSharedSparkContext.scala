package com.b2winc.dmon.util

import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll}

trait AsyncSharedSparkContext extends AsyncWordSpec with BeforeAndAfterAll with BaseSparkContext {

  override protected def beforeAll: Unit = {
    startContext()
    super.beforeAll()
  }

  override protected def afterAll: Unit = {
    stopContext()
    super.afterAll()
  }

}
