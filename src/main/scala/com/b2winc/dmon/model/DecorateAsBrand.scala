package com.b2winc.dmon.model

import com.b2winc.dmon.model.BrandDecorator.{AsBrandUmbrella, AsBrandWms}

trait DecorateAsBrand {

  implicit def asB2wBrandWmsConverter(b: String): AsBrandWms = new AsBrandWms(b)

  implicit def asB2wBrandUmbrellaConverter(b: String): AsBrandUmbrella = new AsBrandUmbrella(b)

}
