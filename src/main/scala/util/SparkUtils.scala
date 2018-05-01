package util

import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

import scala.collection.SortedMap

object SparkUtils {

  def getAlsModelDescription(als:ALS):String={
    val description = SortedMap(
      "alpha"->als.getAlpha,
      "regParam"->als.getRegParam,
      "rank"->als.getRank,
      "maxIter"->als.getMaxIter,
      "userCol"->als.getUserCol,
      "itemCol"->als.getItemCol,
      "ratingCol"->als.getRatingCol,
      "userBlocks"->als.getNumUserBlocks,
      "itemBlocks"->als.getNumItemBlocks,
      "implicitPrefs"->als.getImplicitPrefs
    )
    JsonUtils.toJson(description)
  }

  def getAlsModelSignature(als:ALS):String={
    Math.abs(getAlsModelDescription(als).hashCode).toString
  }

  def getSparkContext():SparkContext={
    getSparkSession().sparkContext
  }

  def getSparkSession():SparkSession={
    SparkSession.builder().getOrCreate()
  }
}