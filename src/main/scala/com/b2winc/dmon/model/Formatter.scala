package com.b2winc.dmon.model

import java.sql.Timestamp

import com.b2winc.dmon.auth.Token
import com.b2winc.dmon.data._
import com.b2winc.dmon.web.{AtlasDependency, Health, ResourceStatus}
import com.github.nscala_time.time.Imports
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import spray.json.DefaultJsonProtocol._
import spray.json.{DefaultJsonProtocol, DeserializationException, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

object Formatter extends DefaultJsonProtocol {

  implicit val dateTimeFormat = Formatter.DateJsonFormat
  implicit val tokenFormat = jsonFormat2(Token)
  implicit val atlasDependencyFormat = jsonFormat4(AtlasDependency)
  implicit val healthFormat = jsonFormat3(Health)
  implicit val resourceStatusFormat = jsonFormat3(ResourceStatus)
  implicit val timestampFormat = Formatter.TimestampFormat
  implicit val saleFormat = jsonFormat7(Sale)
  implicit val salesFormat = jsonFormat1(Sales)
  implicit val stockFormat = jsonFormat6(Stock)
  implicit val stocksFormat = jsonFormat1(Stocks)
  implicit val shipmentFormat = jsonFormat13(Shipment)
  implicit val shipmentsFormat = jsonFormat1(Shipments)

  /**
    * My custom formats
    */

  object DateJsonFormat extends RootJsonFormat[DateTime] {

    private val parserISO : DateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis();

    override def write(obj: DateTime) = JsString(parserISO.print(obj))

    override def read(json: JsValue) : DateTime = json match {
      case JsString(s) => parserISO.parseDateTime(s)
      case _ => throw new DeserializationException("Not possible to deserialize datetime.")
    }
  }

  object TimestampFormat extends RootJsonFormat[Timestamp] {
    def write(obj: Timestamp) = {
      JsString(new DateTime(obj.getTime).toString("yyyy-MM-dd"))
    }

    def read(json: JsValue) = {
      json match {
        case JsString(time) => new Timestamp(DateTime.parse(time, Imports.DateTimeFormat.forPattern("yyyy-MM-dd")).getMillis)
        case _ => throw new DeserializationException("Date expected")
      }
    }
  }
}


