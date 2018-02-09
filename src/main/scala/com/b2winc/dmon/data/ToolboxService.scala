package com.b2winc.dmon.data

import com.b2winc.dmon.util.ConfigurationContext
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.{Failure, Success, Try}

class ToolboxService extends Serializable{

  private def validateInitialDate(initialDate: DateTime) = {
    val skipMaxPeriodValidation = ConfigurationContext.getBoolConfigOrDefault("skipMaxPeriodValidation", false)
    if (!skipMaxPeriodValidation && DateTime.now.minusMonths(1).compareTo(initialDate) > 0) throw new IllegalArgumentException("The specified initialDate is older than 1 month.")
  }

  private def convertStringToDT(dateString: String, dateFormatter: DateTimeFormatter): DateTime = {
    val maybeConvertedDate = Try(DateTime.parse(dateString, dateFormatter))
    maybeConvertedDate match {
      case Success(dateTime) => dateTime
      case Failure(e) => throw new IllegalArgumentException(s"The date $dateString was not specified in a valid format (YYYY-MM-DD).")
    }
  }

  def validateAndConvertDates(initialDate: Option[String], finalDate: Option[String]): (DateTime, DateTime) = {
    require(initialDate.filter(!_.trim.isEmpty).isDefined, "The initialDate was not informed.")
    require(finalDate.filter(!_.trim.isEmpty).isDefined, "The finalDate was not informed.")

    val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
    val initialDateDT = convertStringToDT(s"${initialDate.get} 00:00", dateFormatter)
    validateInitialDate(initialDateDT)
    val finalDateDT = convertStringToDT(s"${finalDate.get} 23:59", dateFormatter)

    (initialDateDT, finalDateDT)
  }
}
