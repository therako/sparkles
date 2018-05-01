package util

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

trait SparklesTask extends Runnable{

  val logger = Logger.getLogger(this.getClass.getName)

  private var session:SparkSession=null

  private val dependencies=mutable.HashSet[SparklesTask]()

  private var taskStatus=SparklesTaskStatus.READY

  def getStatus():SparklesTaskStatus.Status={
    if(taskStatus==SparklesTaskStatus.WAITING){
      var successCount=0
      dependencies.foreach(task=> {
        if(task.taskStatus==SparklesTaskStatus.SUCCESS) {
          successCount+=1
        }
      })
      if(successCount==this.dependencies.size){
        return SparklesTaskStatus.READY
      }
    }
    return taskStatus
  }

  def addDependency(task: SparklesTask): Unit ={
    checkDeadlock(this, task)
    dependencies.add(task)
    if(taskStatus==SparklesTaskStatus.READY)
      taskStatus=SparklesTaskStatus.WAITING
  }

  def getDependencies():List[SparklesTask]={
    dependencies.toList
  }

  private def checkDeadlock(self:SparklesTask, target: SparklesTask):Unit={
    if(self==target){
      throw new RuntimeException("Deadlock")
    }
    target.dependencies.foreach(task=>{
      checkDeadlock(self, task)
    })
  }

  def execute():Unit={
    if(getStatus()==SparklesTaskStatus.READY){
      taskStatus=SparklesTaskStatus.RUNNING
      try{
        run()
      }catch {
        case ex: Exception =>{
          taskStatus=SparklesTaskStatus.ERROR
          logger.fatal("Error has ocurred ", ex)
          System.exit(1)
        }
      }
      taskStatus=SparklesTaskStatus.SUCCESS
    }
  }
}