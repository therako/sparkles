package util

object SparklesTaskStatus extends Enumeration {
  type Status = Value
  val READY, WAITING, RUNNING, SUCCESS, ERROR = Value
}
