package util.cassandra

import java.io.File
import java.util.UUID

import org.apache.spark.sql.Row
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter.ExternalClient
import org.apache.cassandra.io.sstable.{CQLSSTableWriter, SSTableLoader}
import org.apache.cassandra.utils.OutputHandler
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import util.DistributedLock

import scala.collection.mutable
import scala.sys.process._
import scala.util.Random

object ExportToCassandraProcessor{
  def process(it:Iterator[Row], exportConfBC:Broadcast[ExportToCassandraConf]):Iterator[Row]={
    if(it.hasNext){
      val exportConf=exportConfBC.value
      val uuid=UUID.randomUUID().toString
      val dir="/tmp/"+uuid+"/"+exportConf.keyspace+"/"+exportConf.tableName
      new File(dir).mkdirs()

      val writer=CQLSSTableWriter.builder().inDirectory(dir).forTable(exportConf.createTableStatement)
        .using(exportConf.insertStatement).withPartitioner(new Murmur3Partitioner()).build()

      var fieldTypeDesc:Array[String]=null

      while(it.hasNext){
        val row=it.next()

        if(fieldTypeDesc==null){
          fieldTypeDesc=row.schema.fields.map(field=>field.dataType.simpleString)
        }

        val rowValues=new java.util.ArrayList[AnyRef](row.length)
        for(i<-row.schema.fields.indices){
          val v=row.get(i)
          fieldTypeDesc(i) match{
            case "array<array<string>>" => {
              val arr=v.asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[AnyRef]]]
              rowValues.add(arr.toList.map(el=>el.toList.map(el=>el.toString)))
            }
            case "array<string>" => {
              val arr=v.asInstanceOf[mutable.WrappedArray[AnyRef]]
              rowValues.add(arr.toList.map(el=>el.toString))
            }
            case "string" => {
              rowValues.add(v.toString)
            }
            case _=> {
              rowValues.add(v.asInstanceOf[AnyRef])
            }
          }
        }
        writer.addRow(rowValues)
      }
      writer.close()

      val lock=DistributedLock.getLock(exportConf.distLockHost, exportConf.tableName)

      try{

        val conf=new Configuration()
        val cassHosts=exportConf.cassHosts.split(",").toList
        val shuffledCassHosts=Random.shuffle(cassHosts)
        val selectedCassHost=shuffledCassHosts.head
        conf.set("cassandra.output.thrift.address", selectedCassHost)
        conf.set("mapreduce.output.bulkoutputformat.streamthrottlembits", exportConf.throughputInMBits+"")
        conf.set("mapreduce.output.bulkoutputformat.buffersize", exportConf.throughputBufferSize+"")

        new SSTableLoader(new File(dir), new ExternalClient(conf), new OutputHandler.LogOutput).stream().get()

      }finally{
        lock.release()
        ("rm -Rf "+dir)!
      }
    }
    Array[Row]().iterator
  }
}
