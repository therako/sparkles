package util

import java.net.InetAddress
import java.util.Properties

import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}

object DistributedLock {
  val logger: Logger = Logger.getLogger(this.getClass.getName)
  var zkServer:ZooKeeperServerMain=_

  def start():Unit={
    this.synchronized{
      if(zkServer==null){

        logger.info("Start hosting distributed lock on "+InetAddress.getLocalHost().getHostName())

        val zkProperties:Properties=new Properties()
        zkProperties.setProperty("clientPort", "2181")
        zkProperties.setProperty("dataDir", "/tmp/dist-lock-"+System.nanoTime())
        zkProperties.setProperty("maxClientCnxns", "0")

        val quorumConfiguration = new QuorumPeerConfig()
        quorumConfiguration.parseProperties(zkProperties)

        val serverConfig = new ServerConfig()
        serverConfig.readFrom(quorumConfiguration)

        new Thread(() => {
          zkServer = new ZooKeeperServerMain()
          zkServer.runFromConfig(serverConfig)
        }).start()
      }
    }
  }

  def stop():Unit={
    if(zkServer != null) {
      try{
        val method=zkServer.getClass.getDeclaredMethod("shutdown")
        method.setAccessible(true)
        method.invoke(zkServer)
      }finally{
        zkServer=null
        logger.info("Stopped hosting distributed lock on "+InetAddress.getLocalHost().getHostName())
      }
    }
  }


  def getLock(serverHost:String, namespace:String):DistributedLock={
    val zkClient=new ZkClient(serverHost.trim.toLowerCase(), 60000, 60000)
    var success=false
    while(!success){
      try{
        if(!zkClient.exists("/lock-"+namespace)){
          zkClient.createEphemeral("/lock-"+namespace)
          success=true
        }else{
          Thread.sleep(500)
        }
      }catch{
        case _:Throwable=>Thread.sleep(500)
      }
    }
    new DistributedLock(zkClient)
  }
}

class DistributedLock(zkClient: ZkClient){
  def release():Unit={
    zkClient.close()
  }
}