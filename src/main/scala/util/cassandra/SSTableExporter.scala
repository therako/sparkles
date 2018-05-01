package util.cassandra

import java.net.InetAddress

import com.datastax.driver.core.Cluster
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.{FsUtils, SparklesTask}


class SSTableExporter(session:SparkSession, inputPathStr:String, keyspace:String, tableName:String,
                      keys:Array[String], timeToLive:Int, gcGraceSeconds:Int) extends SparklesTask with Runnable{

  override def run(): Unit = {

    val doneFlagPathStr=inputPathStr+"_exported_to_cass"
    val doneFlagPath=new Path(doneFlagPathStr)
    if(!FsUtils.getFs().exists(doneFlagPath)){
      try{
        ExporterLock.getLock(tableName)

        logger.info("Start exporting results to cassandra [ "+ inputPathStr+" ] => "+keyspace+"."+tableName)

        val df=session.read.parquet(inputPathStr)
        val sizeInMB=Math.max(FsUtils.getFs().getContentSummary(new Path(inputPathStr)).getLength/(1024L*1024L), 1L)
        val writePartitions=Math.max(sizeInMB/256, 1).toInt

        createTableFromDataFrame(keyspace, tableName, df, keys, timeToLive, gcGraceSeconds)

        val sparkContext=df.sparkSession.sparkContext
        val sparkConf=sparkContext.getConf

        val cassHosts=sparkConf.get("spark.cassandra.host")
        val throughputInMBits=sparkConf.get("spark.cassandra.throughput_mbits_per_sec").toInt
        val throughputBufferSize=sparkConf.get("spark.cassandra.throughput_buffer_size").toInt

        val fieldsDesc=df.schema.fields.map(f => f.name).mkString(",")
        val valuesHolder=df.schema.fields.map(_ => "?").mkString(",")
        val insertStatement="insert into "+keyspace+"."+tableName+" ("+fieldsDesc+") values ("+valuesHolder+")"
        val createTableStatement=getCreateTableStatement(keyspace, tableName, df, keys, timeToLive, gcGraceSeconds)

        val conf=ExporterConf(
          keyspace, tableName, insertStatement, createTableStatement, cassHosts,
          throughputInMBits,throughputBufferSize, InetAddress.getLocalHost().getHostName()
        )
        val exportConfBC=sparkContext.broadcast[ExporterConf](conf)

        df.repartition(writePartitions).sortWithinPartitions(df.schema.fields(0).name).rdd.mapPartitions(
          it=> SSTableExportProcessor.process(it, exportConfBC)
        ).count()

        exportConfBC.destroy()

        FsUtils.getFs().mkdirs(doneFlagPath)

        logger.info("Finished exporting results to cassandra [ "+ inputPathStr+" ] => "+keyspace+"."+tableName)
      }finally{
        ExporterLock.unlock(tableName)
      }
    }
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
}
