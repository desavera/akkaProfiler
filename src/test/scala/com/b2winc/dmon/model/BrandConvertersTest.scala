package com.b2winc.dmon.model

import org.scalatest.{Matchers, WordSpec}
import BrandConverters._

class BrandConvertersTest extends WordSpec with Matchers{

  "The implicit asBrandUmbrella" should {

    "return SHOP" in {
      "1".asBrandUmbrella shouldEqual ("SHOP")
    }

    "return ACOM" in {
      "2".asBrandUmbrella shouldEqual ("ACOM")
    }

    "return SUBA" in {
      "3".asBrandUmbrella shouldEqual ("SUBA")
    }

    "return SOUB" in {
      "7".asBrandUmbrella shouldEqual ("SOUB")
    }

    "fail gracefuly" in {
      an [IllegalArgumentException] should be thrownBy "X".asBrandUmbrella
    }
  }

  "The implicit asBrandWms" should {

    "return SHOP" in {
      "1".asBrandWms shouldEqual ("SHOP")
    }

    "return ACOM" in {
      "26".asBrandWms shouldEqual ("ACOM")
    }

    "return SUBA" in {
      "29".asBrandWms shouldEqual ("SUBA")
    }

    "fail gracefuly" in {
      an [IllegalArgumentException] should be thrownBy "X".asBrandWms
    }
  }

}
