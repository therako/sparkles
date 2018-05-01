
package util.cassandra

import com.datastax.driver.core.Cluster
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import util.{FsUtils, SparklesTask}

class BatchedRowExporter(session:SparkSession, inputPathStr:String, keyspace:String,
                         table:String, primaryKey:Array[String]) extends SparklesTask with Runnable{

  override def run(): Unit = {
    ExporterLock.getLock(table)

    val doneFlagPathStr=inputPathStr+"_exported_to_cass"
    val doneFlagPath=new Path(doneFlagPathStr)
    if(!FsUtils.getFs().exists(doneFlagPath)){
      val numWriteCores=session.sparkContext.getConf.getInt("spark.cassandra.write.cores", 10)
      val input=session.read.parquet(inputPathStr).repartition(numWriteCores)
      try{
        createTableFromDataFrame(keyspace,table,input, primaryKey, -1, -1)
      }catch{
        case _ :Throwable =>  //ignore ex
      }
      input.write.format("org.apache.spark.sql.cassandra").options(
        Map("keyspace" -> keyspace, "table" -> table)).mode(SaveMode.Append).save()
      FsUtils.getFs().mkdirs(doneFlagPath)
    }
    ExporterLock.unlock(table)
  }

  private def createTableFromDataFrame(keyspace:String, tableName:String, df:DataFrame, keys:Array[String],
                                       timeToLive:Int, gcGraceSeconds:Int)={
    val cluster=createCluster(df.sparkSession)
    val cassSession=cluster.connect()
    try{
      val createTableStatement=getCreateTableStatement(keyspace, tableName, df, keys, timeToLive, gcGraceSeconds)
      cassSession.execute(createTableStatement)
    }finally{
      try{ cassSession.close() }catch{ case _:Throwable => }
      try{ cluster.close() }catch{ case _:Throwable => }
    }
  }

  private def getCreateTableStatement(keyspace:String, tableName:String, df:DataFrame, keys:Array[String],
                                      timeToLive:Int, gcGraceSeconds:Int)={
    val primaryKey=keys.mkString(",")
    val fields = df.schema.map(field => field.name + " " + (field.dataType.simpleString.trim() match {
      case "array<array<string>>" => "LIST<frozen<LIST<text>>>"
      case "array<string>" => "list<text>"
      case "string" => "text"
      case _ => field.dataType.simpleString.trim()
    }))
    val fieldsDesc=fields.mkString(", ")
    var additionParams=""
    if(timeToLive >= 0) {
      additionParams=additionParams + " WITH default_time_to_live = " + timeToLive
    }
    if(gcGraceSeconds >= 0) {
      additionParams=additionParams + " AND gc_grace_seconds = " + gcGraceSeconds
    }
    "CREATE TABLE if not exists "+keyspace+"."+tableName+" ("+fieldsDesc+", PRIMARY KEY ("+primaryKey+"))" + additionParams
  }

  private def createCluster(session:SparkSession):Cluster={
    createCluster(session.sparkContext)
  }

  private def createCluster(sc:SparkContext):Cluster={
    val hosts=sc.getConf.get("spark.cassandra.host")
    var builder=Cluster.builder()
    hosts.split(",").foreach(host=>{
      builder=builder.addContactPoint(host)
    })
    builder.build()
  }
}