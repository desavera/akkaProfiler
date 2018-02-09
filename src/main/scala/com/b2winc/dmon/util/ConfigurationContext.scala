package com.b2winc.dmon.util

import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try

object ConfigurationContext {

  private val config = ConfigFactory.load()

  def getIntConfigOrDefault(configKey: String, default: Int): Int = {
    Try(config.getInt(configKey)).getOrElse(default)
  }

  def getStringConfigOrDefault(configKey: String, default: String): String = {
    Try(config.getString(configKey)).getOrElse(default)
  }

  def getBoolConfigOrDefault(configKey:String, default: Boolean): Boolean = {
    Try(config.getBoolean(configKey)).getOrElse(default)
  }

  def minimumDate(): Timestamp = {
    new Timestamp(
      DateTime.parse(
        ConfigurationContext.getStringConfigOrDefault("minDate", "1900-01-01"),
        DateTimeFormat.forPattern("yyyy-MM-dd")
      ).getMillis
    )
  }

  def minimumDateAsString(): String = {
    ConfigurationContext.getStringConfigOrDefault("minDate", "1900-01-01")
  }

}
