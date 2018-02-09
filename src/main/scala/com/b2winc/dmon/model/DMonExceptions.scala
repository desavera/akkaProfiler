package com.b2winc.dmon.model

class InsecureRequestException(messageToAppend: String = "") extends Throwable{

  def message = s"Invalid request, a security requirement was not satisfied. $messageToAppend"

}

class DataNotAvailableException(messageToAppend: String = "") extends Throwable{

  def message = s"The requested data is not available at this time. $messageToAppend"
}
