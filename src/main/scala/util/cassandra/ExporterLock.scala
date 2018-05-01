package util.cassandra

import java.util.concurrent.Semaphore

import scala.collection.mutable

object ExporterLock {

  private val concurrentCassandraExportJobs=mutable.HashMap[String, Semaphore]()

  private def getSem(tableName:String)={
    this.synchronized{
      var sem:Semaphore=concurrentCassandraExportJobs.getOrElse(tableName,null)
      if(sem==null){
        sem=new Semaphore(1)
        concurrentCassandraExportJobs.put(tableName, sem)
      }
      sem
    }
  }

  def getLock(tableName:String)={
    getSem(tableName).acquire()
  }

  def unlock(tableName:String)={
    getSem(tableName).release()
  }
}
