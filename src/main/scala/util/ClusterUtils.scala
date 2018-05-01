package util

import scala.sys.process._

object ClusterUtils {

  def getClusterId():Int={
    val bootTime= "who -b" !!

    Math.abs(bootTime.hashCode)
  }
}
