package util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JsonUtils {

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k, v) => k.name -> v })
  }

  def toJson(value: Any): String = {
    toJson(value, prettyPrint = false)
  }

  def toJson(value: Any, prettyPrint: Boolean): String = {
    if (!prettyPrint)
      mapper.writeValueAsString(value)
    else
      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }

  def toMap[V](json: String)(implicit m: Manifest[V]) = fromJson[Map[String, V]](json)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }

  def fromJson[T](json: Array[Byte])(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }
}