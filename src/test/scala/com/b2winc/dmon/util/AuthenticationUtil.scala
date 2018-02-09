package com.b2winc.dmon.util

import com.b2winc.dmon.auth.SystemTimeIfc

object AuthenticationUtil{

  object SystemTimeTest extends SystemTimeIfc {
    override def currentTimeInMillis: Long = {
      12345L
    }
  }
  
}
