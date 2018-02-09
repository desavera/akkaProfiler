package com.b2winc.dmon.model

object BrandDecorator {

  class AsBrandWms(b: String){
    def asBrandWms: String = {
      b match {
        case "1" => "SHOP"
        case "26" => "ACOM"
        case "29" => "SUBA"
        case _ => throw new IllegalArgumentException("Invalid brandId specified.")
      }
    }
  }

  class AsBrandUmbrella(b: String){
    def asBrandUmbrella: String = {
      b match {
        case "1" => "SHOP"
        case "2" => "ACOM"
        case "3" => "SUBA"
        case "7" => "SOUB"
        case _ => throw new IllegalArgumentException("Invalid brandId specified.")
      }
    }
  }

}
