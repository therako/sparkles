package util

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}

object FsUtils {

  def getFs():FileSystem={
    val conf=SparkUtils.getSparkContext().hadoopConfiguration
    conf.set("fs.defaultFS","gs://"+conf.get("fs.gs.system.bucket"))
    FileSystem.get(conf)
  }

  def getRootPathStr():String={
    "/sparkles-"+ClusterUtils.getClusterId()+"/"
  }

  def getRootPath():Path={
    new Path(getRootPathStr())
  }

  def generateTmpPath():Path={
    return new Path(getRootPathStr()+"/tmp","tmp-"+UUID.randomUUID().toString.toLowerCase())
  }
}